import pg from "pg";
const { Pool } = pg;

// Use DATABASE_URL if available (Railway provides this), otherwise fall back to local config
const connectionString = process.env.DATABASE_URL;

export const pool = new Pool({
    connectionString,
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
});
