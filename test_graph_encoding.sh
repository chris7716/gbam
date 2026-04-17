#!/usr/bin/env bash
set -euo pipefail

VG="/home/hasitha/data/projects/vg/vg"
GBAM_BINARY="/home/hasitha/data/projects/gbam/target/release/gbam_binary"

WORKDIR="/home/hasitha/data/projects/gbam/graph_test"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# echo "==> [1/9] Downloading hg19 reference..."
# if [ ! -f hg19.fa ]; then
#     wget -c "https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.fa.gz"
#     gunzip -f hg19.fa.gz
#     samtools faidx hg19.fa
# else
#     echo "    hg19.fa already exists, skipping"
# fi

# echo "==> [2/9] Extracting chr22..."
# if [ ! -f chr22.fa ]; then
#     samtools faidx hg19.fa chr22 > chr22.fa
#     samtools faidx chr22.fa
# else
#     echo "    chr22.fa already exists, skipping"
# fi

# echo "==> [3/9] Downloading 1000 Genomes chr22 VCF..."
# VCF="ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"
# if [ ! -f "$VCF" ]; then
#     wget -c "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/${VCF}"
#     wget -c "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/${VCF}.tbi"
# else
#     echo "    VCF already exists, skipping"
# fi

# echo "==> [4/9] Building variation graph..."
# if [ ! -f chr22.vg ]; then
#     "$VG" construct -r chr22.fa -v "$VCF" -R chr22 -t 8 > chr22.vg
# else
#     echo "    chr22.vg already exists, skipping"
# fi

# echo "==> [5/9] Exporting graph to GFA..."
# if [ ! -f chr22.gfa ]; then
#     "$VG" convert -f chr22.vg > chr22.gfa
# else
#     echo "    chr22.gfa already exists, skipping"
# fi

# echo "==> [6/9] Simulating chr22 reads with wgsim..."
# if [ ! -f chr22_reads.fastq ]; then
#     wgsim -N 500000 -1 150 -2 150 -e 0.005 -r 0.001 chr22.fa chr22_sim_1.fastq chr22_sim_2.fastq
#     cat chr22_sim_1.fastq chr22_sim_2.fastq > chr22_reads.fastq
# else
#     echo "    chr22_reads.fastq already exists, skipping"
# fi
# echo "    Reads generated: $(wc -l < chr22_sim_1.fastq) lines ($(( $(wc -l < chr22_sim_1.fastq) / 4 )) reads)"

# echo "==> [7/9] Indexing graph and aligning reads with vg..."
# export TMPDIR="$WORKDIR/tmp"
# mkdir -p "$TMPDIR"
# if [ ! -f chr22.xg ]; then
#     "$VG" index -x chr22.xg chr22.vg
# else
#     echo "    chr22.xg already exists, skipping"
# fi
# if [ ! -f chr22.gcsa ]; then
#     "$VG" prune -k 16 chr22.vg > chr22.pruned.vg
#     "$VG" index -g chr22.gcsa -k 16 -b "$TMPDIR" chr22.pruned.vg
# else
#     echo "    chr22.gcsa already exists, skipping"
# fi
# "$VG" map -f chr22_reads.fastq -x chr22.xg -g chr22.gcsa -t 8 > chr22_aln.gam
# "$VG" convert chr22.xg -G chr22_aln.gam > chr22_aln.gaf
# echo "    Total aligned reads: $(wc -l < chr22_aln.gaf)"
# # Surject GAM back to linear BAM (no bwa needed)
# "$VG" surject -x chr22.xg -b -t 8 chr22_aln.gam | samtools sort -o chr22_reads.bam
# samtools index chr22_reads.bam

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
