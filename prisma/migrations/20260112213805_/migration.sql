/*
  Warnings:

  - You are about to drop the column `labels_applied` on the `processed_email` table. All the data in the column will be lost.
  - You are about to drop the column `labels_removed` on the `processed_email` table. All the data in the column will be lost.
  - You are about to drop the `custom_email_rule` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `default_email_rule_override` table. If the table is not empty, all the data it contains will be lost.
  - Added the required column `history_id` to the `processed_email` table without a default value. This is not possible if the table is not empty.
  - Added the required column `priority` to the `processed_email` table without a default value. This is not possible if the table is not empty.

*/
CREATE EXTENSION IF NOT EXISTS vector;

-- DropForeignKey
ALTER TABLE "custom_email_rule" DROP CONSTRAINT "custom_email_rule_user_id_fkey";

-- DropForeignKey
ALTER TABLE "default_email_rule_override" DROP CONSTRAINT "default_email_rule_override_user_id_fkey";

-- AlterTable
ALTER TABLE "processed_email" DROP COLUMN "labels_applied",
DROP COLUMN "labels_removed",
ADD COLUMN     "extracted_task" TEXT,
ADD COLUMN     "history_id" BIGINT NOT NULL,
ADD COLUMN     "priority" VARCHAR NOT NULL;

-- DropTable
DROP TABLE "custom_email_rule";

-- DropTable
DROP TABLE "default_email_rule_override";

-- DropEnum
DROP TYPE "AssociatedEmailClientCategory";

-- CreateTable
CREATE TABLE "user_email_rule" (
    "id" SERIAL NOT NULL,
    "user_id" INTEGER NOT NULL,
    "description" TEXT NOT NULL,
    "semantic_key" TEXT NOT NULL,
    "name" VARCHAR NOT NULL,
    "mail_label" VARCHAR NOT NULL,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "user_email_rule_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "email_embedding" (
    "id" SERIAL NOT NULL,
    "email_id" VARCHAR NOT NULL,
    "embedding" vector(1024) NOT NULL,

    CONSTRAINT "email_embedding_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "user_email_rule_user_id_idx" ON "user_email_rule"("user_id");

-- CreateIndex
CREATE INDEX "processed_email_user_id_history_id_idx" ON "processed_email"("user_id", "history_id" DESC);

-- AddForeignKey
ALTER TABLE "user_email_rule" ADD CONSTRAINT "user_email_rule_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE CASCADE ON UPDATE CASCADE;
