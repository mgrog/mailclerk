//! `SeaORM` Entity, @generated by sea-orm-codegen 1.0.0-rc.5

use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "email_training")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub user_email: String,
    #[sea_orm(unique)]
    pub email_id: String,
    pub from: String,
    pub subject: String,
    #[sea_orm(column_type = "Text")]
    pub body: String,
    pub ai_answer: String,
    #[sea_orm(column_type = "Float")]
    pub confidence: f32,
    pub heuristics_used: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
