use crate::{U16_SIZE, U32_SIZE, U8_SIZE};
/// This module contains declaration of Fields enum, which is used to ease
/// manipulating BAM record fields. Some of the fields are not in the BAM spec
/// and used exclusively in GBAM crate.
use serde::{Deserialize, Serialize};

use std::slice::Iter;
use std::str::FromStr;

// To avoid visual clutter (no need to write Fields::* each time).
use self::Fields::*;

pub const FIELDS_NUM: usize = 25;
/// Fields which contain data (not index fields).
#[allow(dead_code)]
pub const DATA_FIELDS_NUM: usize = 13;
/// Types of fields contained in BAM file.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[allow(missing_docs)]
pub enum Fields {
    /// Data fields
    #[allow(clippy::upper_case_acronyms)]
    RefID,
    Pos,
    Mapq,
    Bin,
    Flags,
    #[allow(clippy::upper_case_acronyms)]
    NextRefID,
    NextPos,
    TemplateLength,
    ReadName,
    RawCigar,
    RawSequence,
    RawQual,
    RawTags,
    /// Index fields
    LName,
    NCigar,
    SequenceLength,
    RawTagsLen, // Not in BAM spec, needed for index GBAM file
    RawSeqLen,  // Not in BAM spec, needed for index GBAM file
    // Pangenome graph path encoding columns
    PathNodeIds,  // = 18, variable: flat stream of node IDs per read
    PathStart,    // = 19, fixed u32: byte offset into path where alignment begins
    EditOffsets,  // = 20, variable: flat stream of edit positions per read
    EditBases,    // = 21, variable: flat stream of edit bases per read
    NodeCounts,   // = 22, fixed u32: cumulative byte offset index for PathNodeIds
    EditCounts,   // = 23, fixed u32: cumulative byte offset index for EditOffsets
    EditBasesLen, // = 24, fixed u32: cumulative byte offset index for EditBases
}

impl Fields {
    /// Returns iterator over enum fields
    pub fn iterator() -> Iter<'static, Fields> {
        static FIELDS: [Fields; FIELDS_NUM] = [
            // Data fields
            RefID,
            Pos,
            Mapq,
            Bin,
            Flags,
            NextRefID,
            NextPos,
            TemplateLength,
            ReadName,
            RawCigar,
            RawSequence,
            RawQual,
            RawTags,
            // Index fields
            LName,
            NCigar,
            SequenceLength,
            RawSeqLen,
            RawTagsLen,
            // Pangenome graph path encoding columns
            PathNodeIds,
            PathStart,
            EditOffsets,
            EditBases,
            NodeCounts,
            EditCounts,
            EditBasesLen,
        ];
        FIELDS.iter()
    }
}

impl FromStr for Fields {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "RefID" => Ok(Fields::RefID),
            "Pos" => Ok(Fields::Pos),
            "Mapq" => Ok(Fields::Mapq),
            "Bin" => Ok(Fields::Bin),
            "Flags" => Ok(Fields::Flags),
            "NextRefID" => Ok(Fields::NextRefID),
            "NextPos" => Ok(Fields::NextPos),
            "TemplateLength" => Ok(Fields::TemplateLength),
            "ReadName" => Ok(Fields::ReadName),
            "RawCigar" => Ok(Fields::RawCigar),
            "RawSequence" => Ok(Fields::RawSequence),
            "RawQual" => Ok(Fields::RawQual),
            "RawTags" => Ok(Fields::RawTags),
            "LName" => Ok(Fields::LName),
            "NCigar" => Ok(Fields::NCigar),
            "SequenceLength" => Ok(Fields::SequenceLength),
            "RawTagsLen" => Ok(Fields::RawTagsLen),
            "RawSeqLen" => Ok(Fields::RawSeqLen),
            "PathNodeIds" => Ok(Fields::PathNodeIds),
            "PathStart" => Ok(Fields::PathStart),
            "EditOffsets" => Ok(Fields::EditOffsets),
            "EditBases" => Ok(Fields::EditBases),
            "NodeCounts" => Ok(Fields::NodeCounts),
            "EditCounts" => Ok(Fields::EditCounts),
            "EditBasesLen" => Ok(Fields::EditBasesLen),
            _ => Err(()),
        }
    }
}

/// Fields holding index are not data fields
pub fn is_data_field(field: &Fields) -> bool {
    matches!(
        field,
        RefID
            | Pos
            | Mapq
            | Bin
            | Flags
            | NextRefID
            | NextPos
            | TemplateLength
            | ReadName
            | RawCigar
            | RawSequence
            | RawQual
            | RawTags
    )
}

/// Variable sized fields have fixed size value NONE
/// This is **not** BAM sizes!
pub fn field_item_size(field: &Fields) -> Option<usize> {
    match field {
        RefID => Some(U32_SIZE),
        Pos => Some(U32_SIZE),
        LName => Some(U32_SIZE),
        Mapq => Some(U8_SIZE),
        Bin => Some(U16_SIZE),
        NCigar => Some(U32_SIZE),
        Flags => Some(U16_SIZE),
        SequenceLength => Some(U32_SIZE),
        NextRefID => Some(U32_SIZE),
        NextPos => Some(U32_SIZE),
        TemplateLength => Some(U32_SIZE),
        RawTagsLen => Some(U32_SIZE),
        RawSeqLen => Some(U32_SIZE),
        PathStart => Some(U32_SIZE),
        NodeCounts => Some(U32_SIZE),
        EditCounts => Some(U32_SIZE),
        EditBasesLen => Some(U32_SIZE),
        ReadName => None,
        RawCigar => None,
        RawSequence => None,
        RawQual => None,
        RawTags => None,
        PathNodeIds => None,
        EditOffsets => None,
        EditBases => None,
    }
}

impl std::fmt::Display for Fields {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

/// Type of Field.
#[derive(Debug)]
#[allow(missing_docs)]
pub enum FieldType {
    VariableSized,
    FixedSized,
}

// May be useful:
pub fn field_type(field: &Fields) -> FieldType {
    match field {
        // Fixed size fields
        Fields::RefID
        | Fields::Pos
        | Fields::LName
        | Fields::Mapq
        | Fields::Bin
        | Fields::NCigar
        | Fields::Flags
        | Fields::SequenceLength
        | Fields::NextRefID
        | Fields::NextPos
        | Fields::TemplateLength
        | Fields::RawSeqLen
        | Fields::RawTagsLen
        | Fields::PathStart
        | Fields::NodeCounts
        | Fields::EditCounts
        | Fields::EditBasesLen => FieldType::FixedSized,
        // Variable size fields
        Fields::ReadName
        | Fields::RawCigar
        | Fields::RawSequence
        | Fields::RawQual
        | Fields::RawTags
        | Fields::PathNodeIds
        | Fields::EditOffsets
        | Fields::EditBases => FieldType::VariableSized,
    }
}

/// Returns enum name of index field for particular variable sized field
pub fn var_size_field_to_index(field: &Fields) -> Fields {
    match field {
        Fields::ReadName => Fields::LName,
        Fields::RawQual => Fields::SequenceLength,
        Fields::RawSequence => Fields::RawSeqLen,
        Fields::RawTags => Fields::RawTagsLen,
        Fields::RawCigar => Fields::NCigar,
        Fields::PathNodeIds => Fields::NodeCounts,
        Fields::EditOffsets => Fields::EditCounts,
        Fields::EditBases => Fields::EditBasesLen,
        _ => panic!("Unreachable"),
    }
}
