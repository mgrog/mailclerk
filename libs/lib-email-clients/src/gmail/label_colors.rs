use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use google_gmail1::api::LabelColor;
use once_cell::sync::Lazy;

const WHITE: &str = "#ffffff";
const BLACK: &str = "#000000";

// (Name, Bg, Text)
pub const GMAIL_LABEL_COLORS: [(&str, &str, &str); 102] = [
    ("black", "#000000", WHITE),
    ("white", "#ffffff", BLACK),
    ("amber-100", "#ffd6a2", BLACK),
    ("amber-600", "#eaa041", WHITE),
    ("blue-100", "#98d7e4", BLACK),
    ("blue-100", "#c9daf8", BLACK),
    ("blue-200", "#a4c2f4", BLACK),
    ("blue-300", "#4986e7", WHITE),
    ("blue-400", "#3c78d8", WHITE),
    ("blue-500", "#4a86e8", WHITE),
    ("blue-600", "#285bac", WHITE),
    ("blue-700", "#1c4587", WHITE),
    ("blue-800", "#0d3472", WHITE),
    ("blue-green", "#2da2bb", WHITE),
    ("brown-100", "#fdedc1", BLACK),
    ("brown-200", "#fce8b3", BLACK),
    ("brown-800", "#684e07", WHITE),
    ("burgundy", "#7a2e0b", WHITE),
    ("chalk", "#ebdbde", BLACK),
    ("forest-200", "#a0eac9", BLACK),
    ("forest-300", "#89d3b2", BLACK),
    ("forest-400", "#16a766", WHITE),
    ("forest-700", "#1a764d", WHITE),
    ("forest-800", "#04502e", WHITE),
    ("gray-100", "#f3f3f3", BLACK),
    ("gray-200", "#efefef", BLACK),
    ("gray-300", "#e7e7e7", BLACK),
    ("gray-400", "#c2c2c2", BLACK),
    ("gray-500", "#cccccc", BLACK),
    ("gray-600", "#999999", WHITE),
    ("gray-700", "#666666", WHITE),
    ("gray-800", "#464646", WHITE),
    ("gray-900", "#434343", WHITE),
    ("green-1000", "#094228", WHITE),
    ("green-200", "#43d692", WHITE),
    ("green-300", "#42d692", WHITE),
    ("green-400", "#16a765", WHITE),
    ("green-500", "#2a9c68", WHITE),
    ("green-800", "#0b4f30", WHITE),
    ("green-brown", "#594c05", WHITE),
    ("green-yellow", "#fbe983", BLACK),
    ("lavender-300", "#a479e2", BLACK),
    ("lavender-600", "#653e9b", WHITE),
    ("maroon", "#662e37", WHITE),
    ("mint-200", "#68dfa9", BLACK),
    ("mint-300", "#44b984", WHITE),
    ("mint-400", "#3dc789", WHITE),
    ("mint-500", "#149e60", WHITE),
    ("mint-600", "#0b804b", WHITE),
    ("mint-700", "#076239", WHITE),
    ("oatmeal", "#ffc8af", BLACK),
    ("orange", "#ff7537", WHITE),
    ("periwinkle-200", "#b6cff5", BLACK),
    ("periwinkle-400", "#6d9eeb", BLACK),
    ("purple-100", "#e3d7ff", BLACK),
    ("purple-200", "#d0bcf1", BLACK),
    ("purple-300", "#b694e8", BLACK),
    ("purple-400", "#b99aff", BLACK),
    ("purple-500", "#8e63ce", WHITE),
    ("purple-800", "#41236d", WHITE),
    ("purple-900", "#3d188e", WHITE),
    ("red-100", "#f6c5be", BLACK),
    ("red-200", "#f2b2a8", BLACK),
    ("red-300", "#efa093", BLACK),
    ("red-400", "#e66550", WHITE),
    ("red-500", "#fb4c2f", WHITE),
    ("red-600", "#cc3a21", WHITE),
    ("red-700", "#ac2b16", WHITE),
    ("red-800", "#822111", WHITE),
    ("red-900", "#8a1c0a", WHITE),
    ("rose-200", "#cca6ac", BLACK),
    ("rose-300", "#f7a7c0", BLACK),
    ("rose-600", "#e07798", WHITE),
    ("rose-800", "#83334c", WHITE),
    ("salmon-100", "#fcdee8", BLACK),
    ("salmon-200", "#fbd3e0", BLACK),
    ("salmon-300", "#fbc8d9", BLACK),
    ("salmon-400", "#f691b3", BLACK),
    ("salmon-500", "#f691b2", BLACK),
    ("salmon-600", "#b65775", WHITE),
    ("salmon-700", "#994a64", WHITE),
    ("salmon-800", "#711a36", BLACK),
    ("sea-green-100", "#c6f3de", BLACK),
    ("sea-green-200", "#b3efd3", BLACK),
    ("sea-green-300", "#b9e4d0", BLACK),
    ("sea-green-400", "#a2dcc1", BLACK),
    ("straw-100", "#ffe6c7", BLACK),
    ("straw-200", "#ffdeb5", BLACK),
    ("straw-300", "#ffbc6b", BLACK),
    ("straw-400", "#ffad47", WHITE),
    ("straw-500", "#ffad46", BLACK),
    ("straw-700", "#cf8933", WHITE),
    ("straw-800", "#a46a21", WHITE),
    ("straw-900", "#7a4706", WHITE),
    ("teal-800", "#0d3b44", WHITE),
    ("yellow-100", "fef1d1", BLACK),
    ("yellow-200", "#fcda83", BLACK),
    ("yellow-400", "#fad165", BLACK),
    ("yellow-500", "#f2c960", BLACK),
    ("yellow-600", "#d5ae49", WHITE),
    ("yellow-700", "#aa8831", WHITE),
    ("zinc", "#e4d7f5", BLACK),
];

pub struct GmailLabelColorMap {
    reserved: HashMap<String, LabelColor>,
    unreserved: Vec<LabelColor>,
}

impl GmailLabelColorMap {
    pub fn new() -> Self {
        let mut reserved = HashMap::new();
        reserved.insert("uncategorized".to_string(), get_color("gray-100"));
        reserved.insert("political".to_string(), get_color("purple-400"));
        reserved.insert("ads".to_string(), get_color("yellow-500"));
        reserved.insert("finances".to_string(), get_color("green-800"));
        reserved.insert("notices".to_string(), get_color("oatmeal"));
        reserved.insert("receipts".to_string(), get_color("blue-200"));
        reserved.insert("security alerts".to_string(), get_color("orange"));
        reserved.insert("newsletters".to_string(), get_color("blue-800"));
        reserved.insert("flights".to_string(), get_color("blue-400"));
        reserved.insert("orders".to_string(), get_color("blue-green"));
        reserved.insert("social media".to_string(), get_color("mint-200"));
        reserved.insert("career networking".to_string(), get_color("straw-500"));
        reserved.insert("keep".to_string(), get_color("forest-700"));

        let unreserved = GMAIL_LABEL_COLORS
            .iter()
            .filter(|c| !reserved.contains_key(c.0))
            .map(|c| LabelColor {
                background_color: Some(c.1.to_string()),
                text_color: Some(c.2.to_string()),
            })
            .collect();

        Self {
            reserved,
            unreserved,
        }
    }

    fn hash_string_to_index(&self, s: &str) -> usize {
        let n = self.unreserved.len() - 1;
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        let hash = hasher.finish();
        (hash as usize) % n
    }

    pub fn get(&self, label_name: &str) -> LabelColor {
        let reserved_color = self.reserved.get(label_name);
        if let Some(color) = reserved_color {
            return color.clone();
        }

        let index = self.hash_string_to_index(label_name);
        self.unreserved[index].clone()
    }
}

impl Default for GmailLabelColorMap {
    fn default() -> Self {
        Self::new()
    }
}

fn get_color(key: &str) -> LabelColor {
    let map: Lazy<HashMap<String, LabelColor>> = Lazy::new(|| {
        let mut map = HashMap::new();
        for c in GMAIL_LABEL_COLORS.iter() {
            map.insert(
                c.0.to_string(),
                LabelColor {
                    background_color: Some(c.1.to_string()),
                    text_color: Some(c.2.to_string()),
                },
            );
        }
        map
    });

    map.get(key).unwrap().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_string_to_index() {
        let colors = GmailLabelColorMap::new();

        let index = colors.hash_string_to_index("test");
        assert!(index < colors.unreserved.len());
    }

    #[test]
    fn test_get_reserved_color() {
        let colors = GmailLabelColorMap::new();

        let color = colors.get("ads");
        assert_eq!(color.background_color, Some("#f2c960".to_string()));
        assert_eq!(color.text_color, Some("#000000".to_string()));
    }

    #[test]
    fn test_get_unreserved_color() {
        let colors = GmailLabelColorMap {
            reserved: HashMap::new(),
            unreserved: vec![
                LabelColor {
                    background_color: Some("green".to_string()),
                    text_color: Some("black".to_string()),
                },
                LabelColor {
                    background_color: Some("blue".to_string()),
                    text_color: Some("white".to_string()),
                },
                LabelColor {
                    background_color: Some("purple".to_string()),
                    text_color: Some("white".to_string()),
                },
                LabelColor {
                    background_color: Some("orange".to_string()),
                    text_color: Some("white".to_string()),
                },
            ],
        };

        let color1 = colors.get("test1");
        let color2 = colors.get("test2");
        let color3 = colors.get("test1");
        assert_ne!(color1.background_color, color2.background_color);
        assert_eq!(color1.background_color, color3.background_color);
    }
}
