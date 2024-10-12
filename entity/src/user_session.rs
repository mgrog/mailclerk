//! `SeaORM` Entity, @generated by sea-orm-codegen 1.0.0-rc.5

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "user_session")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    #[sea_orm(unique)]
    pub email: String,
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTimeWithTimeZone,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
    pub active: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::processed_daily_summary::Entity")]
    ProcessedDailySummary,
    #[sea_orm(has_many = "super::user_token_usage_stats::Entity")]
    UserTokenUsageStats,
}

impl Related<super::processed_daily_summary::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ProcessedDailySummary.def()
    }
}

impl Related<super::user_token_usage_stats::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UserTokenUsageStats.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
