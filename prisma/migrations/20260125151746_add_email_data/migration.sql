/*
  Warnings:

  - Added the required column `internal_date` to the `processed_email` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "processed_email" ADD COLUMN     "body" TEXT,
ADD COLUMN     "from" TEXT,
ADD COLUMN     "internal_date" BIGINT NOT NULL,
ADD COLUMN     "snippet" TEXT,
ADD COLUMN     "subject" TEXT;
