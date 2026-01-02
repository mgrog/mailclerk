/*
  Warnings:

  - You are about to drop the column `category` on the `custom_email_rule` table. All the data in the column will be lost.
  - Added the required column `name` to the `custom_email_rule` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "custom_email_rule" DROP COLUMN "category",
ADD COLUMN     "name" VARCHAR NOT NULL;
