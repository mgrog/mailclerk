-- AlterTable
ALTER TABLE "user" ADD COLUMN     "last_updated_email_rules" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP;
