/*
  Warnings:

  - Added the required column `is_thread` to the `processed_email` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "processed_email" ADD COLUMN     "is_thread" BOOLEAN NOT NULL;
