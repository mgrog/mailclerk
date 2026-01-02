/*
  Warnings:

  - Added the required column `label_color` to the `custom_email_rule` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "custom_email_rule" ADD COLUMN     "label_color" VARCHAR NOT NULL;
