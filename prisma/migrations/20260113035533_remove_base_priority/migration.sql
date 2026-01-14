/*
  Warnings:

  - You are about to drop the column `base_priority` on the `processed_email` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "processed_email" DROP COLUMN "base_priority";
