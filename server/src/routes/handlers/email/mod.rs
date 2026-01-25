mod feed;
mod modify;
mod read;
mod search;
mod shared;
mod write;

pub use feed::get_feed;
pub use modify::{mark_as_read, mark_as_unread};
pub use read::{get_all, get_message_by_id, get_messages_by_ids};
pub use search::search;
pub use write::send;
