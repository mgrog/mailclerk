/*
  Warnings:

  - Added the required column `base_priority` to the `processed_email` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "processed_email" ADD COLUMN     "base_priority" INTEGER NOT NULL,
ADD COLUMN     "is_read" BOOLEAN NOT NULL DEFAULT false;
