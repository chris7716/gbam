//! Pattern analysis for read names

use super::utils::ByteUtils;

#[derive(Debug, PartialEq)]
pub enum ReadNamePattern {
    Illumina,
    PacBio,
    Custom,
    Unstructured,
}

pub struct ReadNameAnalyzer;

impl ReadNameAnalyzer {
    pub fn detect_pattern(read_names: &[&[u8]]) -> ReadNamePattern {
        if read_names.is_empty() {
            return ReadNamePattern::Unstructured;
        }

        let illumina_count = read_names.iter()
            .filter(|name| Self::is_illumina_pattern(name))
            .count();

        let pacbio_count = read_names.iter()
            .filter(|name| Self::is_pacbio_pattern(name))
            .count();

        let total = read_names.len();
        
        if illumina_count as f64 / total as f64 > 0.8 {
            ReadNamePattern::Illumina
        } else if pacbio_count as f64 / total as f64 > 0.8 {
            ReadNamePattern::PacBio
        } else if Self::has_custom_pattern(read_names) {
            ReadNamePattern::Custom
        } else {
            ReadNamePattern::Unstructured
        }
    }

    fn is_illumina_pattern(name: &[u8]) -> bool {
        // Pattern: instrument:run:flowcell:lane:tile:x:y:UMI:readnum:filtered:control:index
        ByteUtils::count_byte(name, b':') >= 6 && 
        ByteUtils::split_bytes(name, b':').len() >= 7
    }

    fn is_pacbio_pattern(name: &[u8]) -> bool {
        // Pattern: movieName/holeNumber/start_end
        name.contains(&b'/') && 
        ByteUtils::count_byte(name, b'/') == 2
    }

    fn has_custom_pattern(read_names: &[&[u8]]) -> bool {
        // Check for common prefixes or structures
        if read_names.len() < 2 {
            return false;
        }

        let first = read_names[0];
        let common_prefix_len = read_names.iter()
            .map(|name| ByteUtils::common_prefix_length(first, name))
            .min()
            .unwrap_or(0);

        common_prefix_len > first.len() / 3
    }

    pub fn should_tokenize(read_names: &[&[u8]]) -> bool {
        if read_names.len() < 10 {
            return false; // Not worth tokenizing small datasets
        }

        let pattern = Self::detect_pattern(read_names);
        match pattern {
            ReadNamePattern::Illumina | ReadNamePattern::PacBio => true,
            ReadNamePattern::Custom => {
                // Check redundancy level
                ByteUtils::calculate_redundancy(read_names) > 0.3
            },
            ReadNamePattern::Unstructured => false,
        }
    }

    pub fn analyze_efficiency(read_names: &[&[u8]]) -> f64 {
        if read_names.is_empty() {
            return 0.0;
        }

        let pattern = Self::detect_pattern(read_names);
        let redundancy = ByteUtils::calculate_redundancy(read_names);
        
        match pattern {
            ReadNamePattern::Illumina => 0.8 + redundancy * 0.2,
            ReadNamePattern::PacBio => 0.6 + redundancy * 0.4,
            ReadNamePattern::Custom => redundancy,
            ReadNamePattern::Unstructured => redundancy * 0.1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_illumina_detection() {
        let illumina_names: Vec<Vec<u8>> = vec![
            b"HWI-ST1234:123:H0ABCADXX:1:1101:1234:5678:ACGTACGT:1:N:0:ATCACG".to_vec(),
            b"HWI-ST1234:123:H0ABCADXX:1:1101:1234:5679:ACGTACGT:2:N:0:ATCACG".to_vec(),
        ];

        let name_refs: Vec<&[u8]> = illumina_names.iter().map(|v| v.as_slice()).collect();
        let pattern = ReadNameAnalyzer::detect_pattern(&name_refs);
        assert_eq!(pattern, ReadNamePattern::Illumina);
    }

    #[test]
    fn test_pacbio_detection() {
        let pacbio_names: Vec<Vec<u8>> = vec![
            b"movie1/12345/0_1000".to_vec(),
            b"movie1/12346/100_1100".to_vec(),
        ];

        let name_refs: Vec<&[u8]> = pacbio_names.iter().map(|v| v.as_slice()).collect();
        let pattern = ReadNameAnalyzer::detect_pattern(&name_refs);
        assert_eq!(pattern, ReadNamePattern::PacBio);
    }

    #[test]
    fn test_should_tokenize() {
        // Too few reads
        let few_names: Vec<Vec<u8>> = vec![
            b"HWI-ST1234:123:H0ABCADXX:1:1101:1234:5678".to_vec(),
        ];
        let few_refs: Vec<&[u8]> = few_names.iter().map(|v| v.as_slice()).collect();
        assert!(!ReadNameAnalyzer::should_tokenize(&few_refs));

        // Enough Illumina reads
        let many_names: Vec<Vec<u8>> = (0..20)
            .map(|i| format!("HWI-ST1234:123:H0ABCADXX:1:1101:{}:5678:ACGTACGT:1:N:0:ATCACG", 1000 + i).into_bytes())
            .collect();
        let many_refs: Vec<&[u8]> = many_names.iter().map(|v| v.as_slice()).collect();
        assert!(ReadNameAnalyzer::should_tokenize(&many_refs));
    }
}