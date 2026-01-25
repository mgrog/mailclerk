-- AlterTable
ALTER TABLE "user" ADD COLUMN     "daily_token_limit" BIGINT NOT NULL DEFAULT 200000;
