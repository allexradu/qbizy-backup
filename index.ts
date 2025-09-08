import { exec } from 'child_process'
import { promisify } from 'util'
import { config } from 'dotenv'

config({ path: '.dev.vars' })

const execAsync = promisify(exec)
// Helper to extract password from DB URL
export function parsePostgresUrl(dbUrl: string) {
    const url = new URL(dbUrl)
    return {
        host: url.hostname,
        user: url.username,
        password: url.password,
        port: url.port,
        database: url.pathname.replace(/^\//, ''),
    }
}

function getTimestampedFilename(): string {
    const now = new Date()
    const pad = (n: number) => n.toString().padStart(2, '0')
    const DD = pad(now.getDate())
    const MM = pad(now.getMonth() + 1)
    const YYYY = now.getFullYear()
    const HH = pad(now.getHours())
    const mm = pad(now.getMinutes())
    const SS = pad(now.getSeconds())
    return `qbizy_production_${DD}_${MM}_${YYYY}_${HH}:${mm}:${SS}.tar`
}

export async function backupDatabaseToTarFile(): Promise<void> {
    const dbUrl = process.env.DATABASE_URL!
    const outputFile = getTimestampedFilename()
    const password = parsePostgresUrl(dbUrl).password
    const command = `PGPASSWORD=${password} pg_dump --dbname="${dbUrl}" --no-owner --no-privileges --format=tar --file="${outputFile}"`
    await execAsync(command)
}

await backupDatabaseToTarFile()


