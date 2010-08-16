# -*- encoding: utf-8 -*-
# GAE adapter for http://labs.corefive.com/projects/filemanager/

import datetime
import logging
import re
import urllib

from django.utils import simplejson as json
from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import blobstore_handlers
from google.appengine.ext.webapp import template
import google.appengine.ext.webapp.util

from py import constants

# Set this to the same value used for fileRoot in filemanager.config.js
ROOT_PATH = "/filemanager/files"

class FileException(Exception):
  """Any error from one of the file/folder operations."""
class EAlready(FileException):
  """File already exists."""

class Folder(db.Model):
  is_folder = True
  
  date_created = db.DateTimeProperty(auto_now_add=True)
  date_modified = db.DateTimeProperty(auto_now=True)
  
  path = db.StringProperty(required=True)
  
  @classmethod
  def get_by_path(cls, path):
    path = re.sub("//+", "/", path)
    if path != "/":
      path = re.sub(r"/+$", "", path)
    logging.info("Getting folder %s", path)
    ds = cls.all().filter("path =", path).fetch(1)
    if ds:
      return ds[0]
    if path == ROOT_PATH:
      # If the root folder doesn't exist, create it
      path_components = re.findall(r"(/?[^/]+)", path) or [""]
      for p in [ "".join(path_components[:i]) or "/" for i in range(len(path_components)) ]:
        d = cls(path=path)
        d.put()
      
      return d
    
    return None
  
  def get_path(self):
    return self.path
  
  def get_name(self):
    return self.path.split("/")[-1]
  
  def child_folders(self):
    r = []
    prefix = re.sub("//+", "/", self.path + "/")
    for d in self.all().filter("path >", prefix).order("path"):
      if not d.path.startswith(prefix):
        break
      if '/' not in d.path[len(prefix):]:
        r.append(d)
    return r
  
  def parent_path(self):
    mo = re.match(r"(.+)/([^/]+)", self.path)
    if not mo:
      return "/"
    return mo.group(1)
  
  def children(self):
    return self.child_folders() + list(File.all().filter("folder =", self).order("filename"))
  
  def rename_to(self, new_name):
    if self.path == "/":
      raise FileException("You can't rename the root folder")
    old_path = self.path
    parent_path = self.parent_path()
    new_path = re.sub("//+", "/", parent_path + "/" + new_name)
    
    # This is a potential race condition. Worst case, we’ll have to clean up by hand.
    # (The only way to avoid this would be to put all Files and Folders in the same
    # entity group.)
    db.run_in_transaction(self._rename, self.key(), new_path)
    if 1 < self.all().filter("path =", new_path).count(2):
      logging.error("Folder %s already exists", new_path)
      db.run_in_transaction(self._rename, self.key(), old_path)
      raise EAlready("Folder %s already exists", new_path)
    if File.get_by_path(new_path) is not None:
      logging.error("File %s already exists", new_path)
      db.run_in_transaction(self._rename, self.key(), old_path)
      raise EAlready("File %s already exists", new_path)
    self.path = new_path
  
  @classmethod
  def _rename(cls, key, new_path):
    folder = cls.get(key)
    folder.path = new_path
    folder.put()

class File(db.Model):
  is_folder = False
  
  date_created = db.DateTimeProperty(auto_now_add=True)
  date_modified = db.DateTimeProperty(auto_now=True)
  
  folder = db.ReferenceProperty(Folder, required=True)
  filename = db.StringProperty(required=True)
  height = db.IntegerProperty()
  width = db.IntegerProperty()
  
  content = blobstore.BlobReferenceProperty()
  
  def get_path(self):
    if self.folder.path.endswith("/"):
      return self.folder.path + self.filename
    return self.folder.path + "/" + self.filename
  
  def get_name(self):
    return self.filename
  
  def get_extension(self):
    mo = re.match(r".+\.([^.]+)$", self.filename)
    if mo is None:
      return None
    return mo.group(1)
  
  def get_size(self):
    return self.content.size
  
  @classmethod
  def get_by_path(cls, path):
    mo = re.match(r"(.+)/(.+)", path)
    if not mo:
      return None
    folder_path, filename = mo.group(1), mo.group(2)
    folder = Folder.get_by_path(folder_path)
    if not folder:
      return None
    files = cls.all().filter("folder =", folder).filter("filename =", filename).fetch(1)
    if not files:
      return None
    return files[0]
  
  def rename_to(self, new_name):
    old_name = self.filename
    # This is a potential race condition. Worst case, we’ll have to clean up by hand.
    # (The only way to avoid this would be to put all Files and Folders in the same
    # entity group.)
    db.run_in_transaction(self._rename, self.key(), new_name)
    if 1 < self.all().filter("folder =", self.folder).filter("filename =", new_name).count(2):
      logging.error("Duplicate filename %s in folder %s", new_name, self.folder.path)
      db.run_in_transaction(self._rename, self.key(), old_name)
      raise EAlready("Duplicate filename %s in folder %s" % (new_name, self.folder.path))
    if Folder.get_by_path(self.get_path()) is not None:
      logging.error("Path %s is already in use by a folder", self.get_path())
      db.run_in_transaction(self._rename, self.key(), old_name)
      raise EAlready("Path %s is already in use by a folder" % (self.get_path(),))
  
  @classmethod
  def _rename(cls, key, new_name):
    f = cls.get(key)
    f.filename = new_name
    f.put()
  
  def write_to(self, out):
    br = blobstore.BlobReader(self.content.key())
    while True:
      buf = br.read(8192)
      if not buf:
        break
      out.write(buf)

class FileTreeHandler(webapp.RequestHandler):
  def post(self):
    path = urllib.unquote_plus(self.request.get("dir"))
    logging.info("Generating file tree for %s", path)
    folder = Folder.get_by_path(path)
    if folder:
      self.response.out.write(template.render("../templates/filemanager/filetree.tmpl", {
        "listing": folder.children()
      }))
    else:
      self.response.out.write("<i>not found!</i>")

class FileManagerHandler(blobstore_handlers.BlobstoreUploadHandler):
  modes = ["getinfo", "getfolder", "rename", "delete", "addfolder", "download", "getuploadpath", "added"]
  extensions_with_icons = set([
    "aac", "avi", "bmp", "chm", "css", "dll", "doc", "fla", "gif", "htm", "html", "ini", "jar",
    "jpeg", "jpg", "js", "lasso", "mdb", "mov", "mp3", "mpg", "pdf", "php", "png", "ppt", "py",
    "rb", "real", "reg", "rtf", "sql", "swf", "txt", "vbs", "wav", "wma", "wmv", "xls", "xml",
    "xsl", "zip",
  ])
  def get(self):
    mode = self.request.get("mode")
    if mode in self.modes:
      method = getattr(self, mode)
      try:
        response = method()
      except FileException, e:
        logging.exception("FileException in method %s", mode)
        response = {"Error": e.args[0], "Code": -1}
      
      if mode == "added":
        # Yes, seriously.
        self.response.out.write("<textarea>" + json.dumps(response) + "</textarea>")
      elif mode != "download":
        self.response.headers["Content-type"] = "application/json"
        self.response.out.write(json.dumps(response))
    else:
      self.error(500)
  
  def getuploadpath(self):
    return { "Path": blobstore.create_upload_url(self.request.path), "Error": "", "Code": -1 }
  
  def post(self):
    if self.request.get("mode") != "add":
      self.error(405)
      return
    
    path = self.request.get("currentpath")
    uploaded_file = self.get_uploads("newfile")[0]
    
    folder = Folder.get_by_path(path)
    if folder is None:
      self.redirect(self.request.path + "?" + urllib.urlencode({
        "mode": "added",
        "error": "Folder does not exist",
      }))
    
    dirent = File(folder=folder, content=uploaded_file, filename=uploaded_file.filename)
    # xxxx - width/height for images
    dirent.put()
    
    logging.info("path=%s, file=%s", path, uploaded_file)
    self.redirect(self.request.path + "?" + urllib.urlencode({
      "mode": "added",
      "key": str(dirent.key())
    }))
  
  def added(self):
    key_str = self.request.get("key")
    if not key_str:
      return {
        "Error": self.request.get("error"),
        "Code": -1,
      }
    dirent = db.get(db.Key(key_str))
    return {
      "Path": dirent.get_path(),
      "Name": dirent.get_name(),
      "Error": "", "Code": 0,
    }
  
  def _format_datetime(self, dt):
    return None if dt is None else datetime.datetime.strftime(dt, "%Y-%m-%d %H:%M:%S")
  
  def get_dirent_by_path(self, path):
    dirent = Folder.get_by_path(path)
    if dirent is None:
      dirent = File.get_by_path(path)
    return dirent
  
  def getinfo(self, dirent=None):
    if dirent is None:
      path = self.request.get("path")
      dirent = self.get_dirent_by_path(path)
      if dirent is None:
        logging.error("Path not found: %s", path)
        return {"Error": "Not found", "Code": -1}
    
    r = {
      "Path": dirent.get_path(),
      "Filename": dirent.get_name(),
      "Properties": {
        "Date Created": self._format_datetime(dirent.date_created),
        "Date Modified": self._format_datetime(dirent.date_modified),
      },
    }
    if dirent.is_folder:
      r.update({
        "File Type": "dir",
        "Preview": "/filemanager/images/fileicons/_Open.png",
      })
    else:
      icon = "/filemanager/images/fileicons/default.png"
      extension = dirent.get_extension()
      if extension is None:
        extension = "txt"
      elif extension in self.extensions_with_icons:
        icon = "/filemanager/images/fileicons/default.png"
      
      r.update({
        "File Type": extension,
        "Preview": icon,
      })
      r["Properties"].update({
        "Width": dirent.width,
        "Height": dirent.height,
        "Size": dirent.get_size(),
      })
    r.update({"Error": "", "Code": 0})
    return r
  
  def getfolder(self):
    path, show_thumbs = [ self.request.get(x) for x in ["path", "showThumbs"] ]
    d = self.get_dirent_by_path(path)
    if not d:
      raise FileException("Folder %s not found" % (path))
    if not d.is_folder:
      d = d.folder
    
    return dict(
      (dirent.get_path(), self.getinfo(dirent))
      for dirent in d.children()
    )
  
  def addfolder(self):
    path, name = [ self.request.get(x) for x in ["path", "name"] ]
    logging.info("Creating folder: %s/%s", path, name)
    if path.endswith("/"):
      new_path = path + name
    else:
      new_path = path + "/" + name
    parent_folder = Folder.get_by_path(path)
    if parent_folder is None:
      raise FileException("")
    Folder(path = new_path).put()
    return {"Parent": path, "Name": name, "Error": "No error", "Code": 0}
  
  def rename(self):
    old_path, new_name = [ self.request.get(x) for x in ["old", "new"] ]
    
    dirent = self.get_dirent_by_path(old_path)
    if dirent is None:
      return {"Error": "File not found", "Code": -1}
    
    old_name = dirent.get_name()
    dirent.rename_to(new_name)
    return {
      "Old Path": old_path,
      "Old Name": old_name,
      "New Path": dirent.get_path(),
      "New Name": dirent.get_name(),
      "Error": "", "Code": 0
    }
  
  def delete(self):
    path = self.request.get("path")
    dirent = self.get_dirent_by_path(path)
    if dirent is None:
      return {"Error": "File not found", "Code": -1}
    if dirent.is_folder and 0 < len(dirent.children()):
      return {"Error": "Folder not empty", "Code": -1}
    if dirent.is_folder and not path.endswith('/'):
      path += '/'
    dirent.delete()
    return {"Error": "", "Code": 0, "Path": path}
  
  def download(self):
    path = self.request.get("path")
    dirent = self.get_dirent_by_path(path)
    
    if dirent is None or dirent.is_folder:
      self.error(404)
      return
    
    self.response.headers["Content-type"] = dirent.content.content_type
    self.response.headers["Content-disposition"] = "attachment; filename=" + dirent.filename
    dirent.write_to(self.response.out)

class DownloadHandler(webapp.RequestHandler):
  def get(self, path):
    f = File.get_by_path(path)
    if f is None:
      self.error(404)
      return
    
    self.response.headers["Content-type"] = dirent.content.content_type
    f.write_to(self.response.out)

def main():
  handlers = [
    ('/filemanager/scripts/jquery.filetree/connectors/jqueryFileTree.gae', FileTreeHandler),
    ('/filemanager/connectors/gae/filemanager.gae', FileManagerHandler),
    ('/action/f(/.+)', DownloadHandler)
  ]

  webapp.util.run_wsgi_app(
    webapp.WSGIApplication(handlers, debug=constants.DEBUG))

if __name__ == '__main__':
  main()
