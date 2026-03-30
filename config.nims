# config.nims — automatically loaded by nim for all compilations in this project.
# Adds nimble package paths so `nim c -r tests/...` works without `nimble test`.
when defined(nimscript):
  import std/os
  let htspath = gorge("nimble path hts 2>/dev/null")
  if htspath.len > 0 and htspath[0] == '/':
    switch("path", htspath & "/src")
