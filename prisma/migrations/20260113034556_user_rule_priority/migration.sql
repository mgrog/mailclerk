/*
  Warnings:

  - Added the required column `priority` to the `user_email_rule` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "user_email_rule" ADD COLUMN     "priority" INTEGER NOT NULL;
