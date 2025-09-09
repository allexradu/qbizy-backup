import { Buffer } from 'buffer'

export interface AuthResponse {
    apiUrl: string
    authorizationToken: string
    downloadUrl: string
    accountId: string
}

export async function authenticate(): Promise<AuthResponse> {
    const authEncoded = Buffer.from(
        `${process.env.BACKBLAZE_KEY_ID!}:${process.env.BACKBLAZE_APPLICATION_KEY!}`,
    ).toString('base64')
    const authResponse = await fetch(
        'https://api.backblazeb2.com/b2api/v2/b2_authorize_account',
        {
            headers: {
                Authorization: `Basic ${authEncoded}`,
            },
        },
    )

    if (!authResponse.ok) {
        throw new Error(
            `Authorization failed: ${authResponse.status} ${authResponse.statusText}`,
        )
    }

    return (await authResponse.json()) as AuthResponse
}
