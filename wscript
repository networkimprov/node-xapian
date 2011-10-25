import Options
from os import unlink, symlink
from os.path import exists

srcdir = "."
blddir = "build"
VERSION = "0.0.1"

def set_options(opt):
  opt.tool_options("compiler_cxx")

def configure(conf):
  conf.check_tool("compiler_cxx")
  conf.check_tool("node_addon")
  if not conf.check_cfg(package='xapian', args='--cflags --libs', uselib_store='XAPIAN'):
    if not conf.check(lib="xapian", uselib_store="XAPIAN"):
      conf.fatal('Missing Xapian');
#   conf.check_cfg(package='profiler', args='--cflags --libs', uselib_store='SQLITE3')
#   conf.env.append_value('LIBPATH_PROFILER', '/usr/local/lib')
#   conf.env.append_value('LIB_PROFILER', 'profiler')

def build(bld):
  obj = bld.new_task_gen("cxx", "shlib", "node_addon")
  obj.cxxflags = ["-g", "-D_FILE_OFFSET_BITS=64", "-D_LARGEFILE_SOURCE", "-Wall"]
  obj.target = "xapian-binding"
  obj.source = "xapian-binding.cc"
  #obj.lib = "xapian"
  obj.uselib = "XAPIAN PROFILER"

t = 'xapian-binding.node'
def shutdown():
  # HACK to get binding.node out of build directory.
  # better way to do this?
  if Options.commands['clean']:
    if exists(t): unlink(t)
  else:
    if exists('build/default/' + t) and not exists(t):
      symlink('build/default/' + t, t)

