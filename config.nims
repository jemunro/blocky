# config.nims — automatically loaded by nim for all compilations in this project.
when defined(nimscript):
  import std/[os, strutils]
  # Tests: panics off so AssertionDefect is raised (catchable) instead of
  # triggering rawQuit, letting the `timed` template print a FAIL line.
  if projectName().startsWith("test_"):
    switch("panics", "off")
# begin Nimble config (version 2)
when withDir(thisDir(), system.fileExists("nimble.paths")):
  include "nimble.paths"
# end Nimble config
