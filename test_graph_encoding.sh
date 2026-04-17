#!/usr/bin/env bash
set -euo pipefail

VG="/home/hasitha/data/projects/vg/vg"
GBAM_BINARY="/home/hasitha/data/projects/gbam/target/release/gbam_binary"
LITTLE_BAM="/home/hasitha/data/projects/gbam/test_data/little.bam"

WORKDIR="/home/hasitha/data/projects/gbam/graph_test"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "==> [1/9] Downloading hg19 reference..."
if [ ! -f hg19.fa ]; then
    wget -c "https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz"
    gunzip -f hg19.fa.gz
    samtools faidx hg19.fa
else
    echo "    hg19.fa already exists, skipping download"
fi

echo "==> [2/9] Extracting chr22..."
samtools faidx hg19.fa chr22 > chr22.fa
samtools faidx chr22.fa

echo "==> [3/9] Downloading 1000 Genomes chr22 VCF..."
wget -c "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"
wget -c "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz.tbi"
VCF="ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"

echo "==> [4/9] Building variation graph..."
"$VG" construct -r chr22.fa -v "$VCF" -R chr22 -t 8 > chr22.vg

echo "==> [5/9] Exporting graph to GFA..."
"$VG" convert -f chr22.vg > chr22.gfa

echo "==> [6/9] Extracting chr22 reads from little.bam and converting to FASTQ..."
samtools index "$LITTLE_BAM"
samtools view -b "$LITTLE_BAM" chr22 > chr22_reads.bam
READ_COUNT=$(samtools view -c chr22_reads.bam)
echo "    chr22 reads: $READ_COUNT"
if [ "$READ_COUNT" -eq 0 ]; then
    echo "    WARNING: no chr22 reads in little.bam, using all reads instead"
    cp "$LITTLE_BAM" chr22_reads.bam
fi
samtools fastq chr22_reads.bam > chr22_reads.fastq

echo "==> [7/9] Indexing graph and aligning reads with vg..."
export TMPDIR="$WORKDIR/tmp"
mkdir -p "$TMPDIR"
echo "    vg version: $("$VG" version 2>&1 | head -1)"
echo "    chr22.vg size: $(wc -c < chr22.vg) bytes"
set -x
"$VG" index -x chr22.xg chr22.vg
"$VG" prune -k 16 chr22.vg > chr22.pruned.vg
"$VG" index -g chr22.gcsa -k 16 -b "$TMPDIR" chr22.pruned.vg
set +x
"$VG" map -f chr22_reads.fastq -x chr22.xg -g chr22.gcsa -t 8 > chr22_aln.gam
"$VG" convert -F chr22_aln.gam > chr22_aln.gaf

echo "==> [8/9] Encoding BAM to graph-encoded GBAM..."
"$GBAM_BINARY" \
    --convert-to-gbam-graph chr22_reads.bam \
    -o chr22_graph.gbam \
    --gfa chr22.gfa \
    --gaf chr22_aln.gaf \
    --graph-uri chr22.gfa

echo "==> [9/9] Decoding GBAM back to BAM and verifying round-trip..."
"$GBAM_BINARY" \
    --convert-to-bam chr22_graph.gbam \
    -o chr22_restored.bam \
    --gfa chr22.gfa

samtools view chr22_reads.bam  > original.sam
samtools view chr22_restored.bam > restored.sam

if diff -q original.sam restored.sam > /dev/null; then
    echo ""
    echo "PASS: round-trip is lossless"
    echo ""
    ORIG_SIZE=$(wc -c < chr22_reads.bam)
    GBAM_SIZE=$(wc -c < chr22_graph.gbam)
    echo "Original BAM size : $ORIG_SIZE bytes"
    echo "Graph GBAM size   : $GBAM_SIZE bytes"
    printf "Ratio             : %.2fx\n" "$(echo "scale=4; $ORIG_SIZE / $GBAM_SIZE" | bc)"
else
    echo ""
    echo "FAIL: round-trip produced differences"
    diff original.sam restored.sam | head -20
    exit 1
fi
