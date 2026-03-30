#!/usr/bin/env python3
"""
scatter_vcf.py

Split a BGZF-compressed VCF into N roughly-equal-size chunks,
preserving record boundaries and minimizing recompression.
"""

import argparse
import gzip
import io
import logging
import multiprocessing
import os
import struct
import sys
from itertools import accumulate
from bisect import bisect_left
from pathlib import Path

from Bio import bgzf

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def extract_tbi_block_starts(tbi_path: str):
    logger.info(f"Reading TBI index from {tbi_path}")
    data = gzip.open(tbi_path, "rb").read()
    buf = io.BytesIO(data)
    if buf.read(4) != b"TBI\x01":
        raise ValueError("Not a Tabix index")
    hdr = buf.read(32)
    n_ref, *_rest, name_block_len = struct.unpack("<8i", hdr)
    buf.read(name_block_len)
    starts = set()
    for _ in range(n_ref):
        n_bin = struct.unpack("<I", buf.read(4))[0]
        for _ in range(n_bin):
            buf.read(4)  # bin_id
            n_shard = struct.unpack("<I", buf.read(4))[0]
            for _ in range(n_shard):
                beg, end = struct.unpack("<QQ", buf.read(16))
                if end > beg:
                    starts.add(beg >> 16)
        n_intv = struct.unpack("<I", buf.read(4))[0]
        buf.read(8 * n_intv)
    return sorted(starts)

def extract_csi_block_starts(csi_path: str):
    logger.info(f"Reading CSI index from {csi_path}")
    data = gzip.open(csi_path, "rb").read()
    buf = io.BytesIO(data)
    if buf.read(4) != b"CSI\x01":
        raise ValueError("Not a CSI index")
    
    min_shift, depth, l_aux = struct.unpack("<3i", buf.read(12))
    buf.read(l_aux)
    
    n_ref = struct.unpack("<i", buf.read(4))[0]
    starts = set()
    for _ in range(n_ref):
        n_bin = struct.unpack("<i", buf.read(4))[0]
        for _ in range(n_bin):
            buf.read(12) # bin (4) + loffset (8)
            n_chunk = struct.unpack("<i", buf.read(4))[0]
            for _ in range(n_chunk):
                beg, end = struct.unpack("<QQ", buf.read(16))
                if end > beg:
                    starts.add(beg >> 16)
    return sorted(starts)

_scan_bgzf_cache = {}

def scan_bgzf_block_starts(vcf_path: str, start_at=0, end_at=None):
    starts = []
    with open(vcf_path, "rb") as f:
        cur = start_at
        while end_at is None or cur < end_at:
            if cur in _scan_bgzf_cache:
                starts.append(cur)
                cur = _scan_bgzf_cache[cur]
                continue
            f.seek(cur)
            hdr = f.read(12)
            if len(hdr) < 12 or hdr[:3] != b"\x1f\x8b\x08":
                break
            xlen = struct.unpack("<H", hdr[10:12])[0]
            extra = f.read(xlen)
            p = 0
            bsize_m1 = None
            while p + 4 <= xlen:
                si1, si2 = extra[p], extra[p + 1]
                slen = struct.unpack("<H", extra[p + 2 : p + 4])[0]
                if (si1, si2) == (66, 67):  # 'BC'
                    bsize_m1 = struct.unpack("<H", extra[p + 4 : p + 6])[0]
                    break
                p += 4 + slen
            if bsize_m1 is None:
                break
            blk_size = bsize_m1 + 1
            starts.append(cur)
            _scan_bgzf_cache[cur] = cur + blk_size
            cur += blk_size
    return starts

def get_header_and_first_block(vcf_path: str):
    """read in header lines and return offset to first block with records"""
    header_lines = []
    with bgzf.open(vcf_path, "rt") as rdr:
        for line in rdr:
            if line.startswith("#"):
                header_lines.append(line)
            else:
                voff = rdr.tell()
                block_offset = voff >> 16
                header = compress_to_bgzf("".join(header_lines).encode("utf-8"))
                return header, block_offset
    logger.error("No data records found in VCF header")
    
    sys.exit(1)

def is_valid_boundary(vcf_path: str, start:int, length:int):
    """return true if a newline is within the boundary"""
    with bgzf.open(vcf_path, "rb") as rdr:
        rdr.seek(bgzf.make_virtual_offset(start, 0))
        if not rdr.readline().endswith(b'\n'):
            return False
        return (rdr.tell() >> 16) < (start + length)

def compress_to_bgzf(data: bytes, compresslevel: int = 6, eof=False) -> bytes:
    """
    Compress a data object into BGZF format.
    """
    buffer = io.BytesIO()
    writer = bgzf.BgzfWriter(fileobj=buffer, compresslevel=compresslevel)
    writer.write(data)
    if eof:
        buffer.close = lambda *args, **kwargs: None
        writer.flush()
        writer.close()
    else:
        writer.flush()
    return buffer.getvalue()

def split_chunk(vcf_path: str, offset: int, size: int):
    """
    Decompress one a chunk of one of more BGZF blocks at [offset:offset+size],
    split into rougly equal pieces, on a newline
    recompress as bgzf and returns halves
    """
    with open(vcf_path, "rb") as f:
        f.seek(offset)
        lines = gzip.decompress(f.read(size)).splitlines(keepends=True)
    head, tail = map(b"".join, (lines[:len(lines)//2], lines[len(lines)//2:]))
    head = compress_to_bgzf(head, eof=True)
    tail = compress_to_bgzf(tail, eof=False)
    return head, tail

def remove_header(vcf_path: str, offset: int, size: int):
    """
    remove header from one a chunk of one of more BGZF blocks at [offset:offset+size],
    recompress as bgzf
    """
    with open(vcf_path, "rb") as f:
        f.seek(offset)
        lines = gzip.decompress(f.read(size)).splitlines(keepends=True)
    records = b"".join([ln for ln in lines if not ln.startswith(b"#")])
    records = compress_to_bgzf(records)
    return records


def raw_copy_bytes(vcf_path: str, out_fh, start: int, length: int):
    """Copy raw bytes from start to end (exclusive) into out_fh."""
    to_copy = length
    with open(vcf_path, "rb") as f:
        f.seek(start)
        while to_copy > 0:
            chunk = f.read(min(4 * 1024 * 1024, to_copy))
            if not chunk:
                break
            out_fh.write(chunk)
            to_copy -= len(chunk)

def partition_boundaries(lengths, n):
    """
    Return the list of chunk‐indices that should be split
    so that you get as close as possible to n equal‐sized parts.
    """
    if len(lengths) < n-1:
        logger.error(f"Not enough BGZF blocks to partition into {n} chunks. Set --n_shards to {len(lengths) +1 } or lower")
        sys.exit(1)
        
    cum = list(accumulate(lengths))
    total = cum[-1]
    # absolute target positions for the splits
    targets = [(i * total) / n for i in range(1, n)]
    # find which chunk each target lies in
    return [bisect_left(cum, t) for t in targets]

def get_lengths(starts, file_size):
    return [starts[i + 1] - starts[i] for i in range(len(starts) - 1)] + [file_size - starts[-1]]

def optimise_boundaries(vcf_path, starts, n_shards, maxit=1000):
    """
    Find optimal boundary blocks to split VCF into roughly equal size pieces
    """
    logger.info(f"Optimising BGZF block boundaries")
    file_size = os.path.getsize(vcf_path)
    excluded = set()
    scanned = set()
    lengths = get_lengths(starts, file_size)
    bounds = partition_boundaries(lengths, n_shards)  
    i = 0
    while True:
        # scan for finer boundaries
        new_starts = set()
        for s, l in ((starts[i], lengths[i]) for i in bounds):
            if not s in scanned:
                scanned.add(s)
                new_starts = new_starts | set(scan_bgzf_block_starts(str(vcf_path), s, s+l))
        starts = sorted((set(starts) | new_starts) - excluded)
        lengths = get_lengths(starts, file_size)
        bounds = partition_boundaries(lengths, n_shards)
        # check for invalid boundaries 
        invalid = [i for i in bounds if not is_valid_boundary(vcf_path, starts[i], lengths[i])]
        if len(invalid) == 0:
            break
        excluded = excluded | {starts[i] for i in invalid}
        i += 1
        if i == maxit:
            logger.error("Could not find optimal partition, try with smaller --n_shards")
            exit(1)
    
    return bounds, starts, lengths


def write_chunk(vcf_path: str, output_path: Path, prepend: bytes, start: int, length: int, append: bytes):
    # Phase 2: raw copy of complete compressed blocks
    binout = sys.stdout.buffer if output_path == '<stdout>' else open(output_path, "wb")
    if prepend is not None:
        binout.write(prepend)
    if length > 0:
        raw_copy_bytes(vcf_path, binout, start, length)
    if append is not None:
        binout.write(append)
    binout.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("vcf_input",        type=Path,             help="input BGZF VCF (.vcf.gz)")
    p.add_argument("--n-shards",       required=True, type=int, help="number of chunks")
    p.add_argument("-t", "--threads",              type=int, default=1,  help="worker count")
    p.add_argument("-o", "--output",              help="output prefix (e.g. split.vcf.gz)")
    p.add_argument("--chunk",               type=int,        help="1-based chunk number to write")
    p.add_argument("--stdout", action="store_true", help="write selected chunk to stdout (requires --chunk)")
    args = p.parse_args()

    # Validate arguments
    if args.stdout and not args.chunk:
        p.error("--stdout requires --chunk")
    if not args.stdout and not args.output:
        p.error("--output is required unless --stdout is set")
    if args.chunk and (args.chunk < 1 or args.chunk > args.n_shards):
        p.error(f"--chunk must be between 1 and {args.n_shards}")

    vcf_path      = args.vcf_input
    output_prefix = args.output
    n_shards      = args.n_shards

    if (n_shards == 1):
        logger.warning("Setting n_shards to 1 is redundant")

    # gather BGZF block starts
    tbi_path = vcf_path.with_suffix(vcf_path.suffix + ".tbi")
    csi_path = vcf_path.with_suffix(vcf_path.suffix + ".csi")
    if tbi_path.exists():
        starts = extract_tbi_block_starts(str(tbi_path))
    elif csi_path.exists():
        starts = extract_csi_block_starts(str(csi_path))
    else:
        logger.error("Warning: No .tbi or .csi index found - please index file with bcftools or tabix.")
        sys.exit(1)
    logger.info(f"Found {len(starts)} BGZF blocks")

    logger.info(f"Extracting header")
    header, first_block = get_header_and_first_block(str(vcf_path))
    # improve resolution of chopping of first chunk and header
    starts = sorted(set(starts) | {first_block})
    starts = sorted(set(starts + scan_bgzf_block_starts(str(vcf_path), starts[0], starts[1])))    

    file_size = os.path.getsize(vcf_path)
    bounds, starts, lengths = optimise_boundaries(vcf_path, starts, n_shards)

    # build write_args
    logger.info(f"Splitting boundary BGZF blocks")
    bounds_split = [split_chunk(str(vcf_path), starts[i], lengths[i]) for i in bounds]
    write_args = []
    for i in range(n_shards):
        if i == 0:
            if starts[0] < starts[bounds[0]]:
                prepend = header + remove_header(str(vcf_path), 0, starts[1])
            else:
                prepend = header
            start = starts[1]
        else:
            prepend = header + bounds_split[i-1][1]
            start = starts[bounds[i-1]+1]
        if i == n_shards-1:
            length = file_size - start
            append = None
        else:
            length = starts[bounds[i]] - start
            append = bounds_split[i][0]
        if length < 0:
            length = 0
        write_args.append(
            (
                str(vcf_path),
                Path(f"{output_prefix}.{i+1:0{len(str(n_shards))}d}.vcf.gz"),
                prepend, start, length, append
            )
        )
    
    # If only one chunk requested, filter down
    if args.chunk:
        write_args = [write_args[args.chunk - 1]]
    
    if args.threads > 1 and len(write_args) > 1:
        logger.info(f"Writing {len(write_args)} chunks with {args.threads} threads")
        with multiprocessing.Pool(args.threads) as pool:
            pool.starmap(write_chunk, write_args)
    else:
        if args.stdout:
            write_args[0] = tuple([str(vcf_path), '<stdout>'] + list(write_args[0][2:]))
            logger.info(f"Writing chunk {args.chunk} to stdout")
        else:
            logger.info(f"Writing {len(write_args)} chunks")
        for wa in write_args:
            write_chunk(*wa)

    logger.info(f"Done")


if __name__ == "__main__":
    main()
