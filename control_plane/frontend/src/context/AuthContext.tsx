import { createContext, useContext, useEffect, useState, ReactNode } from 'react'
import axios from 'axios'

interface AuthState {
  token:    string | null
  username: string | null
  role:     string | null
}

interface AuthCtx extends AuthState {
  login:  (username: string, password: string) => Promise<void>
  logout: () => void
}

const Ctx = createContext<AuthCtx>({} as AuthCtx)

const TOKEN_KEY = 'orgbrain_token'
const USER_KEY  = 'orgbrain_user'

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>(() => {
    try {
      const raw = localStorage.getItem(USER_KEY)
      return raw ? JSON.parse(raw) : { token: null, username: null, role: null }
    } catch {
      return { token: null, username: null, role: null }
    }
  })

  // Keep axios default header in sync
  useEffect(() => {
    if (state.token) {
      axios.defaults.headers.common['Authorization'] = `Bearer ${state.token}`
    } else {
      delete axios.defaults.headers.common['Authorization']
    }
  }, [state.token])

  // Intercept 401s → auto logout
  useEffect(() => {
    const id = axios.interceptors.response.use(
      r => r,
      err => {
        if (err?.response?.status === 401) logout()
        return Promise.reject(err)
      },
    )
    return () => axios.interceptors.response.eject(id)
  }, [])

  const login = async (username: string, password: string) => {
    const r = await axios.post('/api/auth/login', { username, password })
    const { token, role } = r.data
    const next: AuthState = { token, username, role }
    setState(next)
    localStorage.setItem(USER_KEY, JSON.stringify(next))
    axios.defaults.headers.common['Authorization'] = `Bearer ${token}`
  }

  const logout = () => {
    setState({ token: null, username: null, role: null })
    localStorage.removeItem(USER_KEY)
    delete axios.defaults.headers.common['Authorization']
  }

  return <Ctx.Provider value={{ ...state, login, logout }}>{children}</Ctx.Provider>
}

export const useAuth = () => useContext(Ctx)
