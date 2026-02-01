/*
  Warnings:

  - You are about to drop the column `is_user_customized` on the `user_email_rule` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "user_email_rule" DROP COLUMN "is_user_customized";

-- CreateTable
CREATE TABLE "system_email_rule" (
    "id" SERIAL NOT NULL,
    "description" TEXT NOT NULL,
    "semantic_key" TEXT NOT NULL,
    "name" VARCHAR NOT NULL,
    "mail_label" VARCHAR NOT NULL,
    "extract_tasks" BOOLEAN NOT NULL DEFAULT false,
    "priority" INTEGER NOT NULL,

    CONSTRAINT "system_email_rule_pkey" PRIMARY KEY ("id")
);
