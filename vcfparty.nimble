# Package
version     = "0.1.0"
author      = "munro.j"
description = "Parallel processing of VCF files"
license     = "MIT"
srcDir      = "src"
bin         = @["vcfparty"]

# Dependencies
requires "nim >= 2.0.0"

# ---------------------------------------------------------------------------
# Vendored libdeflate
# ---------------------------------------------------------------------------

const LibdeflateVer = "1.25"
const LibdeflateUrl = "https://github.com/ebiggers/libdeflate/archive/refs/tags/v" &
                      LibdeflateVer & ".tar.gz"
const LibdeflateDir = "vendor/libdeflate-" & LibdeflateVer
const LibdeflateA   = LibdeflateDir & "/build/libdeflate.a"

proc buildLibdeflate() =
  if not fileExists(LibdeflateA):
    mkDir "vendor"
    let tarball = "vendor/libdeflate-" & LibdeflateVer & ".tar.gz"
    if not fileExists(tarball):
      exec "curl -fsSL " & LibdeflateUrl & " -o " & tarball
    exec "tar -xz -C vendor -f " & tarball
    exec "cmake -B " & LibdeflateDir & "/build -S " & LibdeflateDir &
         " -DLIBDEFLATE_BUILD_SHARED_LIBS=OFF -DLIBDEFLATE_BUILD_GZIP=OFF" &
         " -DCMAKE_BUILD_TYPE=Release"
    exec "cmake --build " & LibdeflateDir & "/build --parallel"

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

before build:
  buildLibdeflate()

task release, "Build release binary":
  buildLibdeflate()
  exec "nim c -d:release src/vcfparty.nim"

task test, "Run all tests":
  exec "nimble build"   # triggers before build hook → buildLibdeflate + nim compile
  # Clear test nimcaches so source changes in imported modules are picked up.
  exec "rm -rf nimcache/tests"
  exec "nim c --hints:off -r tests/test_vcf_utils.nim"
  exec "nim c --hints:off -r tests/test_scatter.nim"
  exec "nim c --hints:off -r tests/test_run.nim"
  exec "nim c --hints:off -r tests/test_merge.nim"
  exec "nim c --hints:off -r tests/test_collect.nim"
  exec "nim c --hints:off -r tests/test_gather.nim"
  exec "nim c --hints:off -r tests/test_cli.nim"
  # exec "testament pattern 'tests/test_*.nim'"
