//!Fluentd forward protocol definitions.
use serde::ser::{Serialize, SerializeMap, SerializeTuple, Serializer};

use core::fmt;
use indexmap::IndexMap;
use std::borrow::Cow;
use std::sync::Arc;
use std::time;

#[derive(Clone)]
#[repr(transparent)]
///IndexMap object suitable for fluent record.
pub struct Map(IndexMap<Cow<'static, str>, Value>);

impl Map {
    #[inline(always)]
    ///Creates new empty map.
    pub fn new() -> Self {
        Self(IndexMap::new())
    }

    #[cfg(feature = "callsite_stats")]
    #[inline(always)]
    pub(crate) fn estimated_payload_bytes(&self) -> usize {
        self.0
            .iter()
            .map(|(key, value)| key.len() + value.estimated_payload_bytes())
            .sum()
    }
}

impl core::fmt::Debug for Map {
    #[inline(always)]
    fn fmt(&self, fmt: &mut core::fmt::Formatter) -> core::fmt::Result {
        core::fmt::Debug::fmt(&self.0, fmt)
    }
}

impl core::ops::Deref for Map {
    type Target = IndexMap<Cow<'static, str>, Value>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl core::ops::DerefMut for Map {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub(crate) struct Opts {
    size: usize,
}

#[derive(Clone)]
///Map value type.
pub enum Value {
    ///Boolean
    Bool(bool),
    ///Integer
    Int(i64),
    ///Unsigned integer
    Uint(u64),
    ///String
    Str(&'static str),
    ///Owned string
    String(String),
    ///Shared string
    SharedStr(Arc<str>),
    ///Event level
    EventLevel(tracing_core::Level),
    ///Object
    Object(Map),
}

impl From<bool> for Value {
    #[inline(always)]
    fn from(val: bool) -> Self {
        Self::Bool(val)
    }
}

impl From<i64> for Value {
    #[inline(always)]
    fn from(val: i64) -> Self {
        Self::Int(val)
    }
}

impl From<u32> for Value {
    #[inline(always)]
    fn from(val: u32) -> Self {
        Self::Uint(val as _)
    }
}

impl From<u64> for Value {
    #[inline(always)]
    fn from(val: u64) -> Self {
        Self::Uint(val)
    }
}

impl From<&'static str> for Value {
    #[inline(always)]
    fn from(val: &'static str) -> Self {
        Self::Str(val)
    }
}

impl From<String> for Value {
    #[inline(always)]
    fn from(val: String) -> Self {
        Self::String(val)
    }
}

impl From<Arc<str>> for Value {
    #[inline(always)]
    fn from(val: Arc<str>) -> Self {
        Self::SharedStr(val)
    }
}

impl From<tracing::Level> for Value {
    #[inline(always)]
    fn from(val: tracing::Level) -> Self {
        Self::EventLevel(val)
    }
}

impl From<Map> for Value {
    #[inline(always)]
    fn from(val: Map) -> Self {
        Self::Object(val)
    }
}

impl fmt::Debug for Value {
    #[inline(always)]
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Bool(val) => fmt::Display::fmt(val, fmt),
            Value::Int(val) => fmt::Display::fmt(val, fmt),
            Value::Uint(val) => fmt::Display::fmt(val, fmt),
            Value::EventLevel(val) => fmt::Debug::fmt(val, fmt),
            Value::Str(val) => fmt::Debug::fmt(val, fmt),
            Value::String(val) => fmt::Debug::fmt(val, fmt),
            Value::SharedStr(val) => fmt::Debug::fmt(val, fmt),
            Value::Object(val) => fmt::Debug::fmt(val, fmt),
        }
    }
}

impl Value {
    #[cfg(feature = "callsite_stats")]
    #[inline(always)]
    pub(crate) fn as_str_value(&self) -> Option<&str> {
        match self {
            Value::Str(val) => Some(val),
            Value::String(val) => Some(val.as_str()),
            Value::SharedStr(val) => Some(val.as_ref()),
            Value::EventLevel(val) => Some(tracing_level_to_str(*val)),
            _ => None,
        }
    }

    #[cfg(feature = "callsite_stats")]
    #[inline(always)]
    pub(crate) fn as_u64_value(&self) -> Option<u64> {
        match self {
            Value::Int(val) if *val >= 0 => Some(*val as u64),
            Value::Uint(val) => Some(*val),
            _ => None,
        }
    }

    #[cfg(feature = "callsite_stats")]
    #[inline(always)]
    pub(crate) fn estimated_payload_bytes(&self) -> usize {
        match self {
            Value::Bool(_) => 1,
            Value::Int(val) => val.to_string().len(),
            Value::Uint(val) => val.to_string().len(),
            Value::Str(val) => val.len(),
            Value::String(val) => val.len(),
            Value::SharedStr(val) => val.len(),
            Value::EventLevel(val) => tracing_level_to_str(*val).len(),
            Value::Object(val) => val.estimated_payload_bytes(),
        }
    }
}

#[derive(Debug)]
///Representation of fluent entry within `Message`
pub struct Record {
    time: time::Duration,
    entries: Map,
}

impl Record {
    #[inline(always)]
    ///Creates record with current timestamp
    pub fn now() -> Self {
        let time = match time::SystemTime::now().duration_since(time::SystemTime::UNIX_EPOCH) {
            Ok(time) => time,
            Err(_) => panic!("SystemTime is before UNIX!?"),
        };

        Self {
            time,
            entries: Map::new(),
        }
    }

    #[inline(always)]
    ///Merges record entries with provided map
    pub fn update(&mut self, other: &Map) {
        for (key, value) in other.iter() {
            if !self.entries.contains_key(key) {
                self.entries.insert(key.clone(), value.clone());
            }
        }
    }

    #[cfg(feature = "callsite_stats")]
    #[inline(always)]
    pub(crate) fn estimated_payload_bytes(&self) -> usize {
        self.entries.estimated_payload_bytes() + 16
    }

    #[cfg(feature = "callsite_stats")]
    #[inline(always)]
    pub(crate) fn callsite_key(&self) -> Option<String> {
        let module = self.entries.get("Module")?.as_str_value()?;
        let file = self.entries.get("File")?.as_str_value()?;
        let line = self.entries.get("LineNumber")?.as_u64_value()?;
        Some(format!("{module}|{file}:{line}"))
    }
}

impl core::ops::Deref for Record {
    type Target = Map;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl core::ops::DerefMut for Record {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
}

#[derive(Debug)]
///Forward mode message.
pub struct Message {
    tag: &'static str,
    entries: Vec<Record>,
    opts: Opts,
    //option
}

impl Message {
    #[inline(always)]
    ///Creates new message with provided tag.
    pub const fn new(tag: &'static str) -> Self {
        Self {
            tag,
            entries: Vec::new(),
            opts: Opts { size: 0 },
        }
    }

    #[inline(always)]
    ///Adds record to the message.
    pub fn add(&mut self, record: Record) {
        self.entries.push(record);
        self.opts.size += 1;
    }

    #[inline(always)]
    ///Returns number of records inside message.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(feature = "callsite_stats")]
    #[inline(always)]
    pub(crate) fn records(&self) -> &[Record] {
        self.entries.as_slice()
    }

    #[inline(always)]
    ///Clears records from the message
    pub fn clear(&mut self) {
        self.entries.clear();
        self.opts.size = 0;
    }
}

fn tracing_level_to_str(level: tracing_core::Level) -> &'static str {
    if level == tracing_core::Level::ERROR {
        "ERROR"
    } else if level == tracing_core::Level::WARN {
        "WARN"
    } else if level == tracing_core::Level::INFO {
        "INFO"
    } else if level == tracing_core::Level::DEBUG {
        "DEBUG"
    } else {
        "TRACE"
    }
}

impl Serialize for Value {
    #[inline]
    fn serialize<SER: Serializer>(&self, ser: SER) -> Result<SER::Ok, SER::Error> {
        match self {
            Value::Bool(val) => ser.serialize_bool(*val),
            Value::Int(val) => ser.serialize_i64(*val),
            Value::Uint(val) => ser.serialize_u64(*val),
            Value::EventLevel(val) => ser.serialize_str(tracing_level_to_str(*val)),
            Value::Str(val) => ser.serialize_str(val),
            Value::String(val) => ser.serialize_str(val),
            Value::SharedStr(val) => ser.serialize_str(val),
            Value::Object(val) => {
                let mut map = ser.serialize_map(Some(val.len()))?;
                for (key, value) in val.iter() {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl Serialize for Map {
    #[inline]
    fn serialize<SER: Serializer>(&self, ser: SER) -> Result<SER::Ok, SER::Error> {
        let mut map = ser.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0.iter() {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

impl Serialize for Opts {
    #[inline]
    fn serialize<SER: Serializer>(&self, ser: SER) -> Result<SER::Ok, SER::Error> {
        let mut map = ser.serialize_map(Some(1))?;
        map.serialize_entry("size", &self.size)?;
        map.end()
    }
}

impl Serialize for Record {
    #[inline]
    fn serialize<SER: Serializer>(&self, ser: SER) -> Result<SER::Ok, SER::Error> {
        let mut seq = ser.serialize_tuple(2)?;

        let seconds = self.time.as_secs();
        #[cfg(feature = "event_time")]
        {
            struct Int8([u8; 8]);

            impl Serialize for Int8 {
                #[inline]
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    serializer.serialize_bytes(&self.0)
                }
            }

            //rmpv derives extension type of bytes size
            struct ExtType((i8, Int8));

            impl Serialize for ExtType {
                #[inline]
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    use rmp_serde::MSGPACK_EXT_STRUCT_NAME;

                    serializer.serialize_newtype_struct(MSGPACK_EXT_STRUCT_NAME, &self.0)
                }
            }

            //seq.serialize_element(&self.time.as_secs())?;
            //
            //Serialize time as EventTime ext
            //https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1.5#eventtime-ext-format
            //This is valid up to year 2106
            let nanos = self.time.subsec_nanos();
            let seconds = (seconds as u32).to_be_bytes();
            let nanos = nanos.to_be_bytes();
            let time = [
                seconds[0], seconds[1], seconds[2], seconds[3], nanos[0], nanos[1], nanos[2],
                nanos[3],
            ];
            let time = ExtType((0, Int8(time)));
            seq.serialize_element(&time)?;
        }
        #[cfg(not(feature = "event_time"))]
        {
            seq.serialize_element(&seconds)?;
        }

        seq.serialize_element(&self.entries)?;
        seq.end()
    }
}

impl Serialize for Message {
    #[inline]
    fn serialize<SER: Serializer>(&self, ser: SER) -> Result<SER::Ok, SER::Error> {
        let mut seq = ser.serialize_tuple(3)?;
        seq.serialize_element(&self.tag)?;
        seq.serialize_element(&self.entries)?;
        seq.serialize_element(&self.opts)?;
        seq.end()
    }
}
