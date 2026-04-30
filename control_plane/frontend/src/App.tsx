import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { AuthProvider, useAuth } from './context/AuthContext'
import MainLayout from './layouts/MainLayout'
import Login      from './pages/Login'
import Dashboard  from './pages/Dashboard'
import Services   from './pages/Services'
import DataSources from './pages/DataSources'
import Governance from './pages/Governance'
import Topics     from './pages/Topics'
import Brain      from './pages/Brain'
import Jobs       from './pages/Jobs'
import Agent      from './pages/Agent'
import Ontology   from './pages/Ontology'
import DataLoad   from './pages/DataLoad'

/** Redirect to /login if not authenticated */
function ProtectedLayout() {
  const { token } = useAuth()
  if (!token) return <Navigate to="/login" replace />
  return <MainLayout />
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<Login />} />
      <Route path="/" element={<ProtectedLayout />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="dashboard"  element={<Dashboard />} />
        <Route path="services"   element={<Services />} />
        <Route path="sources"    element={<DataSources />} />
        <Route path="governance" element={<Governance />} />
        <Route path="topics"     element={<Topics />} />
        <Route path="brain"      element={<Brain />} />
        <Route path="jobs"       element={<Jobs />} />
        <Route path="ontology"   element={<Ontology />} />
        <Route path="agent"      element={<Agent />} />
        <Route path="dataload"   element={<DataLoad />} />
      </Route>
    </Routes>
  )
}

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <AppRoutes />
      </BrowserRouter>
    </AuthProvider>
  )
}
