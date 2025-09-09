import { createReadStream, statSync } from 'fs'
import { createHash } from 'crypto'
import { authenticate } from './cdn-authntificate.ts'

interface UploadUrlResponse {
    uploadUrl: string
    authorizationToken: string
}

export interface UploadFileResponse {
    fileId: string
    fileName: string
    accountId: string
    bucketId: string
    contentType?: string
}

/** Tunables */
const LARGE_FILE_THRESHOLD_BYTES = 100 * 1024 * 1024; // 100 MB
const PART_SIZE_BYTES = 100 * 1024 * 1024;            // 100 MB (min 5MB; <= 5GB)
const MAX_RETRIES = 3;

async function getUploadUrl(): Promise<UploadUrlResponse> {
    const authData = await authenticate()
    const res = await fetch(`${authData.apiUrl}/b2api/v2/b2_get_upload_url`, {
        method: 'POST',
        headers: {
            Authorization: authData.authorizationToken,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            bucketId: process.env.BACKBLAZE_BUCKET_ID!,
        }),
    })
    if (!res.ok) {
        throw new Error(`Failed to get upload URL: ${res.status} ${await res.text()}`)
    }
    return (await res.json()) as UploadUrlResponse
}

async function startLargeFile(fileName: string, contentType: string) {
    const authData = await authenticate()
    const res = await fetch(`${authData.apiUrl}/b2api/v2/b2_start_large_file`, {
        method: 'POST',
        headers: {
            Authorization: authData.authorizationToken,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            bucketId: process.env.BACKBLAZE_BUCKET_ID!,
            fileName,
            contentType,
        }),
    })
    if (!res.ok) {
        throw new Error(`Failed to start large file: ${res.status} ${await res.text()}`)
    }
    return await res.json() as { fileId: string }
}

async function getUploadPartUrl(fileId: string): Promise<UploadUrlResponse> {
    const authData = await authenticate()
    const res = await fetch(`${authData.apiUrl}/b2api/v2/b2_get_upload_part_url`, {
        method: 'POST',
        headers: {
            Authorization: authData.authorizationToken,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ fileId }),
    })
    if (!res.ok) {
        throw new Error(`Failed to get upload part URL: ${res.status} ${await res.text()}`)
    }
    return await res.json() as UploadUrlResponse
}

async function finishLargeFile(fileId: string, partSha1Array: string[]) {
    const authData = await authenticate()
    const res = await fetch(`${authData.apiUrl}/b2api/v2/b2_finish_large_file`, {
        method: 'POST',
        headers: {
            Authorization: authData.authorizationToken,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ fileId, partSha1Array }),
    })
    if (!res.ok) {
        throw new Error(`Failed to finish large file: ${res.status} ${await res.text()}`)
    }
    return await res.json() as UploadFileResponse
}

function sha1Hex(buffer: Buffer) {
    const h = createHash('sha1')
    h.update(buffer)
    return h.digest('hex')
}

export async function uploadSmallFileTar(fileName: string, filePath: string): Promise<UploadFileResponse> {
    const uploadData = await getUploadUrl()
    const fileStats = statSync(filePath)

    // Read the entire file into memory to avoid chunked encoding
    const fileBuffer = await new Promise<Buffer>((resolve, reject) => {
        const chunks: Buffer[] = []
        const rs = createReadStream(filePath)
        rs.on('data', (chunk) => chunks.push(chunk))
        rs.on('end', () => resolve(Buffer.concat(chunks)))
        rs.on('error', reject)
    })

    // Calculate SHA1 from the buffer
    const fileHash = sha1Hex(fileBuffer)

    const res = await fetch(uploadData.uploadUrl, {
        method: 'POST',
        headers: {
            Authorization: uploadData.authorizationToken,
            'Content-Type': 'application/x-tar',
            'Content-Length': fileStats.size.toString(),
            'X-Bz-File-Name': encodeURIComponent(fileName),
            'X-Bz-Content-Sha1': fileHash,
        },
        body: fileBuffer,
    })
    return await res.json() as UploadFileResponse
}

async function uploadLargeFileTar(fileName: string, filePath: string): Promise<UploadFileResponse> {
    const contentType = 'application/x-tar'
    const { fileId } = await startLargeFile(fileName, contentType)

    // You can reuse the same part upload URL/token for multiple parts
    let partUrlData = await getUploadPartUrl(fileId)

    const stream = createReadStream(filePath, { highWaterMark: PART_SIZE_BYTES })
    const partSha1Array: string[] = []

    let partNumber = 1
    for await (const chunk of stream) {
        const partSha1 = sha1Hex(chunk)
        let attempt = 0, ok = false, lastErr = ''

        while (attempt < MAX_RETRIES && !ok) {
            attempt++
            const res = await fetch(partUrlData.uploadUrl, {
                method: 'POST',
                headers: {
                    Authorization: partUrlData.authorizationToken,
                    'X-Bz-Part-Number': String(partNumber),
                    'Content-Length': String(chunk.length),
                    'X-Bz-Content-Sha1': partSha1,
                    // Content-Type may be omitted or set to octet-stream for parts
                    'Content-Type': 'application/octet-stream',
                },
                body: chunk as any,
            })

            if (res.ok) {
                ok = true
            } else {
                const status = res.status
                const body = await res.text()
                lastErr = `part ${partNumber} (attempt ${attempt}) failed: ${status} ${body}`
                // If the part URL/token expired (e.g., 401/403), refresh it and retry
                if (status === 401 || status === 403) {
                    partUrlData = await getUploadPartUrl(fileId)
                }
                if (!ok && attempt < MAX_RETRIES) {
                    // basic backoff
                    await new Promise(r => setTimeout(r, 500 * attempt))
                }
            }
        }

        if (!ok) {
            throw new Error(`Giving up on ${lastErr}`)
        }

        partSha1Array.push(partSha1)
        partNumber++
    }

    // Tell B2 to assemble the file
    const finished = await finishLargeFile(fileId, partSha1Array)
    return finished
}

export async function uploadFile(
    fileName: string,
    filePath: string,
): Promise<UploadFileResponse> {
    const stats = statSync(filePath)

    if (stats.size >= LARGE_FILE_THRESHOLD_BYTES) {
        // Large TAR → multipart B2 flow
        return await uploadLargeFileTar(fileName, filePath)
    } else {
        // Small TAR → simple upload
        return await uploadSmallFileTar(fileName, filePath)
    }
}
