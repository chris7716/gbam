#!/usr/bin/env python3                                                                                                          
"""                                                                                                                           
  Generate a synthetic GFA + GAF from little.bam for testing graph-path encoding.
                                                                                                                                  
  Each read becomes one node in the graph (trivial graph, no population structure).                                               
  Every read maps to its own node with a perfect match (zero edits).                                                              
                                                                                                                                  
  This tests encode→decode correctness without requiring vg or GraphAligner.                                                      
"""                                                                                                                             
                                                                                                                                  
import pysam                                                                                                                  
import argparse

def generate(bam_path: str, gfa_path: str, gaf_path: str):                                                                      
    with pysam.AlignmentFile(bam_path, "rb") as bam, \
        open(gfa_path, "w") as gfa, \                                                                                          
        open(gaf_path, "w") as gaf:                                                                                          
                                                                                                                                
        gfa.write("H\tVN:Z:1.0\n")                                                                                              

        node_id = 1                                                                                                             
        for read in bam.fetch(until_eof=True):                                                                                
            seq = read.query_sequence                                                                                           
            if not seq:          # skip unmapped with no sequence
                continue                                                                                                        
                                                                                                                            
            seq_upper = seq.upper()                                                                                             
            seq_len   = len(seq_upper)                                                                                        
                                                                                                                                
            # GFA segment line: S <id> <sequence>                                                                               
            gfa.write(f"S\t{node_id}\t{seq_upper}\n")                                                                           
                                                                                                                                
            # GAF alignment line (12 required columns)                                                                        
            # Col 6 is the path string: >node_id                                                                                
            gaf.write(                                                                                                          
                f"{read.query_name}\t"   # col 1  query name                                                                    
                f"{seq_len}\t"           # col 2  query length                                                                  
                f"0\t"                   # col 3  query start                                                                   
                f"{seq_len}\t"           # col 4  query end                                                                   
                f"+\t"                   # col 5  strand                                                                        
                f">{node_id}\t"          # col 6  path                                                                          
                f"{seq_len}\t"           # col 7  path length
                f"0\t"                   # col 8  path start                                                                    
                f"{seq_len}\t"           # col 9  path end                                                                      
                f"{seq_len}\t"           # col 10 residue matches
                f"{seq_len}\t"           # col 11 block length                                                                  
                f"60\n"                  # col 12 mapq                                                                        
            )                                                                                                                   
            node_id += 1                                                                                                      
                                                                                                                                
    print(f"Wrote {node_id - 1} nodes to {gfa_path}")
    print(f"Wrote {node_id - 1} alignments to {gaf_path}")                                                                      
                                                                                                                                
if __name__ == "__main__":
    parser = argparse.ArgumentParser()                                                                                          
    parser.add_argument("--bam", default="test_data/little.bam")                                                              
    parser.add_argument("--gfa", default="test_data/synthetic.gfa")                                                             
    parser.add_argument("--gaf", default="test_data/synthetic.gaf")
    args = parser.parse_args()                                                                                                  
    generate(args.bam, args.gfa, args.gaf)