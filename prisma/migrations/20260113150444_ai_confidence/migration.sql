/*
  Warnings:

  - Added the required column `ai_confidence` to the `processed_email` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "processed_email" ADD COLUMN     "ai_confidence" DOUBLE PRECISION NOT NULL;
