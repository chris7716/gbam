//! Dictionary management for read name components

use std::collections::HashMap;

/// Tokenized representation of a read name
#[derive(Debug, Clone, PartialEq)]
pub struct TokenizedReadName {
    pub instrument_id: u8,
    pub run_id: u32,
    pub flowcell_id: u8,
    pub lane: u8,
    pub tile: u16,
    pub x_coord: u32,
    pub y_coord: u32,
    pub umi_id: Option<u16>,
    pub read_num: u8,
    pub flags: u8,
    pub index_id: Option<u8>,
}

/// Dictionary for storing common read name components
#[derive(Debug, Clone)]
pub struct ReadNameDictionary {
    pub instruments: Vec<Vec<u8>>,
    pub flowcells: Vec<Vec<u8>>,
    pub umis: Vec<Vec<u8>>,
    pub indices: Vec<Vec<u8>>,
    // Reverse lookup maps for encoding
    instrument_map: HashMap<Vec<u8>, u8>,
    flowcell_map: HashMap<Vec<u8>, u8>,
    umi_map: HashMap<Vec<u8>, u16>,
    index_map: HashMap<Vec<u8>, u8>,
}

impl ReadNameDictionary {
    pub fn new() -> Self {
        Self {
            instruments: Vec::new(),
            flowcells: Vec::new(),
            umis: Vec::new(),
            indices: Vec::new(),
            instrument_map: HashMap::new(),
            flowcell_map: HashMap::new(),
            umi_map: HashMap::new(),
            index_map: HashMap::new(),
        }
    }

    pub fn add_instrument(&mut self, instrument: &[u8]) -> u8 {
        let instrument_vec = instrument.to_vec();
        if let Some(&id) = self.instrument_map.get(&instrument_vec) {
            return id;
        }
        
        if self.instruments.len() >= 255 {
            // Return existing ID if we've hit the limit
            return 254;
        }
        
        let id = self.instruments.len() as u8;
        self.instrument_map.insert(instrument_vec.clone(), id);
        self.instruments.push(instrument_vec);
        id
    }

    pub fn add_flowcell(&mut self, flowcell: &[u8]) -> u8 {
        let flowcell_vec = flowcell.to_vec();
        if let Some(&id) = self.flowcell_map.get(&flowcell_vec) {
            return id;
        }
        
        if self.flowcells.len() >= 255 {
            return 254;
        }
        
        let id = self.flowcells.len() as u8;
        self.flowcell_map.insert(flowcell_vec.clone(), id);
        self.flowcells.push(flowcell_vec);
        id
    }

    pub fn add_umi(&mut self, umi: &[u8]) -> u16 {
        let umi_vec = umi.to_vec();
        if let Some(&id) = self.umi_map.get(&umi_vec) {
            return id;
        }
        
        if self.umis.len() >= 65535 {
            return 65534;
        }
        
        let id = self.umis.len() as u16;
        self.umi_map.insert(umi_vec.clone(), id);
        self.umis.push(umi_vec);
        id
    }

    pub fn add_index(&mut self, index: &[u8]) -> u8 {
        let index_vec = index.to_vec();
        if let Some(&id) = self.index_map.get(&index_vec) {
            return id;
        }
        
        if self.indices.len() >= 255 {
            return 254;
        }
        
        let id = self.indices.len() as u8;
        self.index_map.insert(index_vec.clone(), id);
        self.indices.push(index_vec);
        id
    }

    pub fn get_instrument(&self, id: u8) -> Option<&[u8]> {
        self.instruments.get(id as usize).map(|v| v.as_slice())
    }

    pub fn get_flowcell(&self, id: u8) -> Option<&[u8]> {
        self.flowcells.get(id as usize).map(|v| v.as_slice())
    }

    pub fn get_umi(&self, id: u16) -> Option<&[u8]> {
        self.umis.get(id as usize).map(|v| v.as_slice())
    }

    pub fn get_index(&self, id: u8) -> Option<&[u8]> {
        self.indices.get(id as usize).map(|v| v.as_slice())
    }

    pub fn total_size(&self) -> usize {
        self.instruments.iter().map(|v| v.len()).sum::<usize>() +
        self.flowcells.iter().map(|v| v.len()).sum::<usize>() +
        self.umis.iter().map(|v| v.len()).sum::<usize>() +
        self.indices.iter().map(|v| v.len()).sum::<usize>()
    }

    pub fn entry_counts(&self) -> (usize, usize, usize, usize) {
        (
            self.instruments.len(),
            self.flowcells.len(),
            self.umis.len(),
            self.indices.len(),
        )
    }
}

impl Default for ReadNameDictionary {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_operations() {
        let mut dict = ReadNameDictionary::new();
        
        // Test adding and retrieving instruments
        let id1 = dict.add_instrument(b"HWI-ST1234");
        let id2 = dict.add_instrument(b"HWI-ST1234"); // Should return same ID
        let id3 = dict.add_instrument(b"NextSeq");
        
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
        assert_eq!(dict.get_instrument(id1), Some(b"HWI-ST1234".as_slice()));
        assert_eq!(dict.get_instrument(id3), Some(b"NextSeq".as_slice()));
    }

    #[test]
    fn test_dictionary_size_calculation() {
        let mut dict = ReadNameDictionary::new();
        dict.add_instrument(b"HWI-ST1234");  // 10 bytes
        dict.add_flowcell(b"H0ABCADXX");     // 8 bytes
        
        assert_eq!(dict.total_size(), 18);
    }
}