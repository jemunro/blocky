# Package
version     = "26.4.0"
author      = "munro.j"
description = "Parallel processing of bgzipped files"
license     = "MIT"
srcDir      = "src"
bin         = @["blocky"]

# Dependencies
requires "nim >= 2.0.0"

# ---------------------------------------------------------------------------
# Vendored libdeflate
# ---------------------------------------------------------------------------

const LibdeflateDir = "vendor/libdeflate"
const LibdeflateA   = LibdeflateDir & "/build/libdeflate.a"

proc buildLibdeflate() =
  if not fileExists(LibdeflateA):
    if not fileExists(LibdeflateDir & "/CMakeLists.txt"):
      exec "git submodule update --init --recursive " & LibdeflateDir
    exec "cmake -B " & LibdeflateDir & "/build -S " & LibdeflateDir &
         " -DLIBDEFLATE_BUILD_SHARED_LIBS=OFF" &
         " -DLIBDEFLATE_BUILD_GZIP=OFF" &
         " -DCMAKE_BUILD_TYPE=Release"
    exec "cmake --build " & LibdeflateDir & "/build --parallel"

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

before build:
  buildLibdeflate()

task release, "Build release binary":
  --define:release
  --define:strip
  --panics:on
  setCommand "build"

task test, "Run all tests":
  exec "nimble build -y"  # CLI tests shell out to the binary
  # Clear test nimcaches so source changes in imported modules are picked up.
  exec "rm -rf nimcache/tests"
  # run individual tests
  exec "nim c --hints:off -r tests/test_bgzf.nim"
  exec "nim c --hints:off -r tests/test_scatter.nim"
  exec "nim c --hints:off -r tests/test_run.nim"
  exec "nim c --hints:off -r tests/test_gather.nim"
  exec "nim c --hints:off -r tests/test_cli.nim"
