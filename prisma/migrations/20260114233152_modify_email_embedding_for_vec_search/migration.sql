/*
  Warnings:

  - Added the required column `chunk_index` to the `email_embedding` table without a default value. This is not possible if the table is not empty.
  - Added the required column `chunk_text` to the `email_embedding` table without a default value. This is not possible if the table is not empty.
  - Added the required column `user_id` to the `email_embedding` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "email_embedding" ADD COLUMN     "chunk_index" INTEGER NOT NULL,
ADD COLUMN     "chunk_text" TEXT NOT NULL,
ADD COLUMN     "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN     "user_id" INTEGER NOT NULL;
