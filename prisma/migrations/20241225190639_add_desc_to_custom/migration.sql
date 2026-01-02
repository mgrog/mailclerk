/*
  Warnings:

  - Added the required column `description` to the `custom_email_rule` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "custom_email_rule" ADD COLUMN     "description" TEXT NOT NULL;
