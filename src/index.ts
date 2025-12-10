import express, { Request, Response } from "express";
import { Pool } from "pg";

// ========= ENV =========
const DATABASE_URL = process.env.DATABASE_URL;
const PG_TABLE_NAME = process.env.PG_TABLE_NAME || "airtable_contacts";
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET; // opcional

if (!DATABASE_URL) {
    console.error("DATABASE_URL is not set");
    process.exit(1);
}

const pool = new Pool({
    connectionString: DATABASE_URL,
});

const app = express();
app.use(express.json());

// ========= TIPOS =========
type AirtablePayload = {
    id: string;
    fields: Record<string, any>;
};

const FIELD_MAP: Record<string, string> = {
    "record_id": "record_id",
    "Startup name": "Startup_Name",
    "PH1_Constitution_Location": "PH1_Constitution_Location",
    "date_sourced": "date_sourced",
};

// ========= HELPERS SQL =========
function buildValuesFromFields(fields: Record<string, any>): any[] {
    return Object.keys(FIELD_MAP).map((airtableField) => fields[airtableField]);
}

function buildInsertQuery(): string {
    const cols = ["airtable_id", ...Object.values(FIELD_MAP)];
    const colList = cols.join(", ");
    const placeholders = cols.map((_, i) => `$${i + 1}`).join(", ");
    return `INSERT INTO ${PG_TABLE_NAME} (${colList}) VALUES (${placeholders})`;
}

function buildUpdateQuery(): string {
    const setClause = Object.values(FIELD_MAP)
        .map((col, idx) => `${col} = $${idx + 1}`)
        .join(", ");
    const whereIndex = Object.values(FIELD_MAP).length + 1;
    return `UPDATE ${PG_TABLE_NAME} SET ${setClause} WHERE airtable_id = $${whereIndex}`;
}

async function findRecordById(airtableId: string) {
    const res = await pool.query(
        `SELECT * FROM ${PG_TABLE_NAME} WHERE airtable_id = $1`,
        [airtableId]
    );
    return res.rows[0] || null;
}

async function createRecordInPostgres(
    airtableId: string,
    fields: Record<string, any>
) {
    const values = [airtableId, ...buildValuesFromFields(fields)];
    const query = buildInsertQuery();
    await pool.query(query, values);
}

async function updateRecordInPostgres(
    airtableId: string,
    fields: Record<string, any>
) {
    const values = [...buildValuesFromFields(fields), airtableId];
    const query = buildUpdateQuery();
    await pool.query(query, values);
}

async function syncAirtableRecord(payload: AirtablePayload): Promise<string> {
    const airtableId = payload.id;
    const fields = payload.fields || {};
    const existing = await findRecordById(airtableId);
    
    if (!existing) {
        await createRecordInPostgres(airtableId, fields);
        return "created record";
    } else {
        await updateRecordInPostgres(airtableId, fields);
        return "updated record";
    }
}

// ========= ENDPOINTS =========
app.get("/health", (_req: Request, res: Response) => {
    res.json({ status: "ok" });
});

app.post("/airtable-webhook", async (req: Request, res: Response) => {
    try {
        // Seguridad opcional
        if (WEBHOOK_SECRET) {
            const header = req.header("X-Webhook-Secret");
            if (header !== WEBHOOK_SECRET) {
                return res.status(401).json({ error: "Invalid webhook secret" });
            }
        }

        const payload = req.body as AirtablePayload;
        if (!payload?.id || !payload?.fields) {
            return res.status(400).json({ error: "Invalid payload" });
        }

        const action = await syncAirtableRecord(payload);
        res.json({ success: true, action });
    } catch (err: any) {
        console.error(err);
        res.status(500).json({ error: err.message || "Internal server error" });
    }
});

// ========= ARRANQUE =========
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});