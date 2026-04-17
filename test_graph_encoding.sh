#!/usr/bin/env bash
set -euo pipefail

VG="/home/hasitha/data/projects/vg/vg"
GBAM_BINARY="/home/hasitha/data/projects/gbam/target/release/gbam_binary"

WORKDIR="/home/hasitha/data/projects/gbam/graph_test"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

echo "==> [1/9] Downloading hg19 reference..."
if [ ! -f hg19.fa ]; then
    wget -c "https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz"
    gunzip -f hg19.fa.gz
    samtools faidx hg19.fa
else
    echo "    hg19.fa already exists, skipping"
fi

echo "==> [2/9] Extracting chr22..."
if [ ! -f chr22.fa ]; then
    samtools faidx hg19.fa chr22 > chr22.fa
    samtools faidx chr22.fa
else
    echo "    chr22.fa already exists, skipping"
fi

echo "==> [3/9] Downloading 1000 Genomes chr22 VCF..."
VCF="ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"
if [ ! -f "$VCF" ]; then
    wget -c "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/${VCF}"
    wget -c "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/${VCF}.tbi"
else
    echo "    VCF already exists, skipping"
fi

echo "==> [4/9] Building variation graph..."
if [ ! -f chr22.vg ]; then
    "$VG" construct -r chr22.fa -v "$VCF" -R chr22 -t 8 > chr22.vg
else
    echo "    chr22.vg already exists, skipping"
fi

echo "==> [5/9] Exporting graph to GFA..."
if [ ! -f chr22.gfa ]; then
    "$VG" convert -f chr22.vg > chr22.gfa
else
    echo "    chr22.gfa already exists, skipping"
fi

echo "==> [6/9] Simulating chr22 reads from graph paths with vg sim..."
export TMPDIR="$WORKDIR/tmp"
mkdir -p "$TMPDIR"
if [ ! -f chr22.xg ]; then
    "$VG" index -x chr22.xg chr22.vg
else
    echo "    chr22.xg already exists, skipping"
fi
# vg sim generates reads that follow actual haplotype paths in the graph.
# --align-out produces GAM with path annotations so we skip vg map entirely.
echo "    Simulating 500k reads from graph haplotype paths..."
"$VG" sim -x chr22.xg -n 3000000 -l 150 -e 0.001 -i 0.0001 --align-out -t 8 > chr22_aln.gam
echo "    Reads simulated: $(wc -c < chr22_aln.gam) bytes (GAM)"

echo "==> [7/9] Converting GAM to GAF and BAM..."
"$VG" convert chr22.xg -G chr22_aln.gam > chr22_aln.gaf
echo "    Total reads in GAF: $(wc -l < chr22_aln.gaf)"
"$VG" surject -x chr22.xg -b -t 8 chr22_aln.gam | samtools sort -o chr22_reads.bam
samtools index chr22_reads.bam
echo "    BAM size: $(wc -c < chr22_reads.bam) bytes"

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

samtools view chr22_reads.bam > original.sam
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
