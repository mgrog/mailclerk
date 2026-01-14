/*
  Warnings:

  - Added the required column `has_new_reply` to the `processed_email` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "processed_email" ADD COLUMN     "has_new_reply" BOOLEAN NOT NULL;
