# Package
version     = "0.1.0"
author      = "munro.j"
description = "Parallel processing of VCF files"
license     = "MIT"
srcDir      = "src"
bin         = @["paravar"]

# Dependencies
requires "nim >= 2.0.0"
requires "hts >= 0.3.21"

# Tasks
task test, "Run all tests":
  exec "testament pattern 'tests/test_*.nim'"
