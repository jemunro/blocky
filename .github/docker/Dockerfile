ARG BASE_IMAGE
FROM ${BASE_IMAGE}
COPY blocky /usr/local/bin/blocky
RUN blocky --help
ADD https://github.com/brentp/vcfanno/releases/download/v0.3.7/vcfanno_linux64 /usr/local/bin/vcfanno
RUN chmod +x /usr/local/bin/vcfanno && vcfanno 2>&1 | head -1
