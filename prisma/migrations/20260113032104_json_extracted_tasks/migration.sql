/*
  Warnings:

  - You are about to drop the column `extracted_task` on the `processed_email` table. All the data in the column will be lost.

*/
-- AlterTable
ALTER TABLE "processed_email" DROP COLUMN "extracted_task",
ADD COLUMN     "extracted_tasks" JSONB[] DEFAULT ARRAY[]::JSONB[];
