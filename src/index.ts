import { exec } from 'child_process'
import { promisify } from 'util'
import { config } from 'dotenv'
import { uploadSmallFileTar } from './utils/upload-file'

config({ path: '../dev.vars' })

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

function getTimestampedFilename(database: string): string {
    const now = new Date()
    const pad = (n: number) => n.toString().padStart(2, '0')
    const DD = pad(now.getDate())
    const MM = pad(now.getMonth() + 1)
    const YYYY = now.getFullYear()
    const HH = pad(now.getHours())
    const mm = pad(now.getMinutes())
    const SS = pad(now.getSeconds())
    return `${database}_${DD}_${MM}_${YYYY}_${HH}:${mm}:${SS}.tar`
}

export async function backupDatabaseToTarFile(): Promise<string> {
    console.log('Backup started...')
    const dbUrl = process.env.DATABASE_URL!
    const password = parsePostgresUrl(dbUrl).password
    const { host, user, port, database } = parsePostgresUrl(dbUrl)
    const endpointId = host.split('.')[0]
    const portArg = port ? `:${port}` : ''
    const dbUri = `postgresql://${user}:${password}@${host}${portArg}/${database}?options=endpoint%3D${endpointId}`
    const outputFile = `/tmp/backup/${getTimestampedFilename(database)}`

    const command = `PGSSLMODE=require pg_dump --dbname="${dbUri}" --no-owner --no-privileges --format=tar --file="${outputFile}"`
    await execAsync(command)
    console.log('Backup completed:', outputFile)
    return outputFile
}

async function runDailyBackupLoop() {
    let lastBackup = Date.now() - 24 * 60 * 60 * 1000 // force backup on first run
    while (true) {
        const now = Date.now()
        if (now - lastBackup >= 24 * 60 * 60 * 1000) {
            const filePath = await backupDatabaseToTarFile()
            const database = parsePostgresUrl(process.env.DATABASE_URL!).database

            const date = new Date()
            const pad = (n: number) => n.toString().padStart(2, '0')
            const DD = pad(date.getDate())
            const MM = pad(date.getMonth() + 1)
            const YYYY = date .getFullYear()

            const fileName = `${database}/${YYYY}/${MM}/${DD}/${getTimestampedFilename(database)}`
            const response = await uploadSmallFileTar(fileName, filePath)
            console.log('Uploaded backup to Backblaze B2:', response)
            // delete local file
            await execAsync(`rm -f "${filePath}"`)
            console.log('Deleted local backup file:', filePath)
            lastBackup = now
        }
        await new Promise(res => setTimeout(res, 60 * 1000)) // sleep 1 minute
    }
}

await runDailyBackupLoop()


