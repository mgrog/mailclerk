/*
  Warnings:

  - Added the required column `thread_id` to the `processed_email` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "processed_email" ADD COLUMN     "thread_id" VARCHAR NOT NULL;
