//! Coordinate delta encoding for read names

#[derive(Debug, Clone)]
pub struct CoordinateDeltas {
    pub x_delta: i16,
    pub y_delta: i16,
    pub tile_delta: i16,
}

pub struct CoordinateEncoder {
    last_x: u32,
    last_y: u32,
    last_tile: u16,
}

impl CoordinateEncoder {
    pub fn new() -> Self {
        Self {
            last_x: 0,
            last_y: 0,
            last_tile: 0,
        }
    }

    pub fn encode_coordinates(&mut self, x: u32, y: u32, tile: u16) -> CoordinateDeltas {
        let deltas = CoordinateDeltas {
            x_delta: (x as i32 - self.last_x as i32).clamp(i16::MIN as i32, i16::MAX as i32) as i16,
            y_delta: (y as i32 - self.last_y as i32).clamp(i16::MIN as i32, i16::MAX as i32) as i16,
            tile_delta: (tile as i32 - self.last_tile as i32).clamp(i16::MIN as i32, i16::MAX as i32) as i16,
        };

        self.last_x = x;
        self.last_y = y;
        self.last_tile = tile;

        deltas
    }

    pub fn decode_coordinates(&mut self, deltas: &CoordinateDeltas) -> (u32, u32, u16) {
        self.last_x = (self.last_x as i32 + deltas.x_delta as i32).max(0) as u32;
        self.last_y = (self.last_y as i32 + deltas.y_delta as i32).max(0) as u32;
        self.last_tile = (self.last_tile as i32 + deltas.tile_delta as i32).max(0) as u16;

        (self.last_x, self.last_y, self.last_tile)
    }

    pub fn reset(&mut self) {
        self.last_x = 0;
        self.last_y = 0;
        self.last_tile = 0;
    }

    pub fn get_state(&self) -> (u32, u32, u16) {
        (self.last_x, self.last_y, self.last_tile)
    }
}

impl Default for CoordinateEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinate_delta_encoding() {
        let mut encoder = CoordinateEncoder::new();
        
        let coords = vec![
            (1000, 2000, 1101),
            (1001, 2000, 1101),
            (1002, 2001, 1101),
            (1000, 2010, 1102),
        ];

        let mut deltas = Vec::new();
        for (x, y, tile) in coords.iter() {
            deltas.push(encoder.encode_coordinates(*x, *y, *tile));
        }

        // Verify first delta is absolute values
        assert_eq!(deltas[0].x_delta, 1000);
        assert_eq!(deltas[0].y_delta, 2000);
        assert_eq!(deltas[0].tile_delta, 1101);

        // Verify subsequent deltas are differences
        assert_eq!(deltas[1].x_delta, 1);   // 1001 - 1000
        assert_eq!(deltas[1].y_delta, 0);   // 2000 - 2000
        assert_eq!(deltas[1].tile_delta, 0); // 1101 - 1101

        // Reset and decode
        encoder.reset();
        for (i, delta) in deltas.iter().enumerate() {
            let (x, y, tile) = encoder.decode_coordinates(delta);
            assert_eq!((x, y, tile), coords[i]);
        }
    }

    #[test]
    fn test_coordinate_overflow_protection() {
        let mut encoder = CoordinateEncoder::new();
        
        // Test with values that would overflow i16
        let large_coord = u32::MAX;
        let deltas = encoder.encode_coordinates(large_coord, large_coord, u16::MAX);
        
        // Should be clamped to i16::MAX
        assert_eq!(deltas.x_delta, i16::MAX);
        assert_eq!(deltas.y_delta, i16::MAX);
        assert_eq!(deltas.tile_delta, i16::MAX);
    }
}