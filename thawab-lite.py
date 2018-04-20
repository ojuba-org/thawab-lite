#! /usr/bin/env python
import os
import sys
import re
import subprocess
import logging
import gi

from six import string_types
from six.moves.queue import Queue, Empty

from threading import Thread

gi.require_version("Gtk", "3.0")
from gi.repository import Gtk, GLib

import pypyodbc as pyodbc
#try: import pypyodbc as pyodbc
#except: import pyodbc

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger(__name__)
HERE = os.path.realpath(os.path.dirname(__file__))
GLADE_FN = os.path.join(HERE, "thawab-lite.glade")
if not os.path.exists(GLADE_FN): GLADE_FN=os.path.join(HERE, "../share/thawab/thawab-lite.glade")

py   = sys.version_info
py3k = py >= (3, 0, 0)

if py3k:
    basestring = str
    unicode = str
else:
    bytes = str
    unicode = unicode

str_types = (unicode, bytes)

def touni(s, enc='utf8', err='strict'):
    return s.decode(enc, err) if isinstance(s, str_types) else unicode(s)

def tob(s, enc='utf-8'):
    return s.encode(enc) if isinstance(s, str_types) else bytes(s)

def cell_decode(a):
    return touni(a) if isinstance(a, str_types) else a

def row_to_dict(row, cols):
    return dict([(i,cell_decode(j)) for i,j in zip(cols, row)])

def try_int(i, fallback=None):
    try: return int(i)
    except ValueError: pass
    except TypeError: pass
    return fallback

os.environ['MDB_JET3_CHARSET'] = 'cp1256'
os.environ['MDB_ICONV'] = 'UTF-8'

sql_comments_re = re.compile(r'^--.*$', re.M)
schema_re = re.compile(r'create +table +([^\(]*) +\((.*)\);', re.I | re.M | re.S)

def get_table_col(filename, table_name):
    """
    because ODBC's cursor.columns(table_name) is broken, we use command line
    """
    out = subprocess.check_output(["mdb-schema", "-T", table_name, filename, 'mysql'])
    out = sql_comments_re.sub(u'', touni(out))
    schemas=schema_re.findall(out)
    if not schemas: raise KeyError('table not found')
    cols = [ c.strip().split()[0].strip('`[]') for c in schemas[0][1].split(',') ]
    return cols

file_dlg = None

def get_filename(parent=None):
    global file_dlg
    if file_dlg:
        file_dlg.set_transient_for(parent)
        if file_dlg.run()!=Gtk.ResponseType.ACCEPT: return None
        return file_dlg.get_filename()
    file_dlg = Gtk.FileChooserDialog(
        "Select files to import",
         parent = parent,
         buttons=(
            Gtk.STOCK_CANCEL,
            Gtk.ResponseType.REJECT,
            Gtk.STOCK_OK,
            Gtk.ResponseType.ACCEPT))
    ff = Gtk.FileFilter()
    ff.set_name('Shamela BOK files')
    ff.add_pattern('*.[Bb][Oo][Kk]')
    file_dlg.add_filter(ff)
    ff = Gtk.FileFilter()
    ff.set_name('All files')
    ff.add_pattern('*')
    file_dlg.add_filter(ff)
    file_dlg.set_select_multiple(False)
    file_dlg.connect('delete-event', lambda w,*a: w.hide() or True)
    file_dlg.connect('response', lambda w,*a: w.hide() or True)
    if file_dlg.run()!=Gtk.ResponseType.ACCEPT: return None
    return file_dlg.get_filename()

class MyApp(object):
    instances = 0
    def __init__(self, filename=None):
        MyApp.instances+=1
        self.file_dlg = None
        self.info = None
        self.page_id = 0
        self.keep_running = True
        self.queue = Queue()
        thread = Thread(target=self.worker_loop)
        thread.daemon = True
        thread.start()
        
        builder = Gtk.Builder()
        builder.add_from_file(GLADE_FN)
        builder.connect_signals(self)
        self.window = builder.get_object("main_win")
        self.header = builder.get_object("header")
        self.body = builder.get_object("body")
        self.toc_store = builder.get_object("toc_store")
        self.toc_tree = builder.get_object("toc_tree")
        self.window.show()
        if filename is not None:
            self.open(filename)

    def on_info_btn_clicked(self, w):
        self.goto_page(0)
    
    def on_previous_btn_clicked(self, w):
        self.goto_page(max(0, self.page_id-1))

    def on_next_btn_clicked(self, w):
        self.goto_page(self.page_id+1)
    
    def on_search_entry_activate(self, w):
        text = w.get_text()
        page_id = try_int(text)
        if page_id is not None: self.goto_page(page_id)

    def worker_loop(self):
        while self.keep_running:
            try: a = self.queue.get(timeout=10)
            except Empty: continue
            cb_name, kwargs = a
            cb=getattr(self, cb_name)
            if not cb:
                self.queue.task_done()
                continue
            try: cb(**kwargs)
            except Exception as e: 
                logger.error("ERROR: %r", e)
            self.queue.task_done()
        logger.info("worker thread exited")

    def open(self, filename):
        self.filename = filename
        cols = get_table_col(filename, 'Main')
        self.db = db = pyodbc.connect(
            tob('DRIVER=libmdbodbc.so;DBQ={}'.format(filename)),
            readonly=True, ansi=True, unicode_results=False,
        )
        cursor = db.cursor()
        cursor.execute(u'SELECT {} FROM Main'.format(','.join(cols)))
        self.info = row_to_dict(cursor.fetchone(), cols)
        self.header.set_title(self.info['Bk'])
        self.goto_page(0)
        #cols = cursor.columns('Main') # does not work
        self.id = int(self.info['BkId'])
        cursor = db.cursor()
        tbl_toc = 't{}'.format(self.info['BkId'])
        cols = get_table_col(filename, tbl_toc)
        cursor.execute(u'SELECT {} FROM {}'.format(','.join(cols), tbl_toc))
        rows = [ row_to_dict(row, cols) for row in cursor.fetchall() ]
        rows.sort(key=lambda r:(r['id'], r['sub']))
        def cb(r):
            for row in r: self.toc_store.append(None, (row['tit'], row['lvl'], row['sub'],  row['id'],))
        # it's a store, not UI, so we might be able to edit it directly
        # cb(rows)
        # if not then it's added like this
        GLib.idle_add(cb, rows)

    def goto_page(self, page_id, move_toc=False):
        if self.info is None: return
        self.page_id = page_id
        if page_id==0:
            text = self.info['Betaka']
        else:
            tbl_body = 'b{}'.format(self.info['BkId'])
            cols = get_table_col(self.filename, tbl_body)
            cursor = self.db.cursor()
            cursor.execute(u'SELECT {} FROM {} WHERE id={}'.format(','.join(cols), tbl_body, page_id))
            self.page = row_to_dict(cursor.fetchone(), cols)
            text = self.page['nass']
        GLib.idle_add(lambda: self.body.get_buffer().set_text(text))

    def on_window_destroy(self, w):
        self.keep_running = False
        MyApp.instances-=1
        logger.info("running instances = %r", MyApp.instances)
        if MyApp.instances==0:
            Gtk.main_quit()

    def on_toc_tree_selection_changed(self, w):
        s, i = w.get_selected()
        # can be accessed in many ways row=tuple(s[i]) or id=s[i][3] or id=s.get_value(i, 3)
        self.queue.put(('goto_page', {'page_id': s[i][3]},))

    def on_open_btn_clicked(self, w):
        filename = get_filename(self.window)
        if filename:
            if self.info is None:
                self.queue.put(('open', {'filename': filename},))
            else:
                MyApp(filename)

files = sys.argv[1:]
if not files:
    MyApp()
else:
    for f in files:
        MyApp(f)
Gtk.main()

