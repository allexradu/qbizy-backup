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

export async function restoreDatabaseFromTarFile(): Promise<void> {
    console.log('Restore started...')
    const dbUrl = process.env.DATABASE_URL!
    const restoreFile = process.env.RESTORE_FILE_NAME
    if (!restoreFile) throw new Error('RESTORE_FILE_NAME env not set')
    const password = parsePostgresUrl(dbUrl).password
    const { host, user, port, database } = parsePostgresUrl(dbUrl)
    const endpointId = host.split('.')[0]
    const portArg = port ? `:${port}` : ''
    const dbUri = `postgresql://${user}:${password}@${host}${portArg}/${database}?options=endpoint%3D${endpointId}`
    const filePath = `/tmp/backup/${restoreFile}`

    const command = `PGSSLMODE=require pg_restore --dbname="${dbUri}" --no-owner --no-privileges --format=tar "${filePath}"`
    await execAsync(command)
    console.log('Restore completed:', filePath)
}

await restoreDatabaseFromTarFile()


