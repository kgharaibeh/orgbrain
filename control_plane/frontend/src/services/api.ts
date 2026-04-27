import axios from 'axios'

const api = axios.create({ baseURL: '/api', timeout: 30000 })

// ── Services ─────────────────────────────────────────────────────────────────
export const servicesApi = {
  list:    ()          => api.get('/services'),
  start:   (key: string) => api.post(`/services/${key}/start`),
  stop:    (key: string) => api.post(`/services/${key}/stop`),
  restart: (key: string) => api.post(`/services/${key}/restart`),
  logs:    (key: string, tail = 200) => api.get(`/services/${key}/logs?tail=${tail}`),
  status:  (key: string) => api.get(`/services/${key}/status`),
}

// ── Data Sources / Connectors ─────────────────────────────────────────────────
export const sourcesApi = {
  list:    ()              => api.get('/connectors'),
  create:  (data: any)     => api.post('/connectors', data),
  get:     (id: number)    => api.get(`/connectors/${id}`),
  update:  (id: number, data: any) => api.put(`/connectors/${id}`, data),
  delete:  (id: number)    => api.delete(`/connectors/${id}`),
  pause:   (id: number)    => api.post(`/connectors/${id}/pause`),
  resume:  (id: number)    => api.post(`/connectors/${id}/resume`),
  restart: (id: number)    => api.post(`/connectors/${id}/restart`),
  status:  (id: number)    => api.get(`/connectors/${id}/status`),
  plugins: ()              => api.get('/connectors/connect/plugins'),
}

// ── Kafka Topics ──────────────────────────────────────────────────────────────
export const topicsApi = {
  list:             ()          => api.get('/topics'),
  create:           (data: any) => api.post('/topics', data),
  delete:           (name: string) => api.delete(`/topics/${name}`),
  consumerGroups:   ()          => api.get('/topics/consumer-groups'),
  provisionDefaults:()          => api.post('/topics/provision-defaults'),
}

// ── Governance ────────────────────────────────────────────────────────────────
export const governanceApi = {
  listSources:    ()                        => api.get('/governance/sources'),
  createSource:   (data: any)               => api.post('/governance/sources', data),
  deleteSource:   (id: number)              => api.delete(`/governance/sources/${id}`),
  listRules:      (sourceId: number)        => api.get(`/governance/sources/${sourceId}/rules`),
  createRule:     (sourceId: number, data: any) => api.post(`/governance/sources/${sourceId}/rules`, data),
  updateRule:     (ruleId: number, data: any) => api.put(`/governance/rules/${ruleId}`, data),
  deleteRule:     (ruleId: number)          => api.delete(`/governance/rules/${ruleId}`),
  exportRules:    (sourceId: number)        => api.get(`/governance/rules/export/${sourceId}`),
  auditLog:       (params?: any)            => api.get('/governance/audit', { params }),
  auditSummary:   ()                        => api.get('/governance/audit/summary'),
}

// ── Brain ─────────────────────────────────────────────────────────────────────
export const brainApi = {
  stats:          ()                          => api.get('/brain/stats'),
  entityCounts:   ()                          => api.get('/brain/graph/entity-counts'),
  recentActivity: (limit = 20)                => api.get(`/brain/graph/recent-activity?limit=${limit}`),
  signals:        (signalType?: string, limit = 50) =>
    api.get('/brain/timeseries/signals', { params: { ...(signalType ? { signal_type: signalType } : {}), limit } }),
  highRisk:       (limit = 50)                => api.get(`/brain/timeseries/high-risk?limit=${limit}`),
  activity:       ()                          => api.get('/brain/timeseries/activity'),
  initQdrant:     ()                          => api.post('/brain/qdrant/init-collections'),
  initNeo4j:      ()                          => api.post('/brain/neo4j/init-schema'),
}

// ── Jobs ──────────────────────────────────────────────────────────────────────
export const jobsApi = {
  list:        ()                          => api.get('/jobs'),
  submit:      (id: number, cfg?: any)     => api.post(`/jobs/${id}/submit`, cfg ?? {}),
  cancel:      (id: number)                => api.post(`/jobs/${id}/cancel`),
  metrics:     (id: number)                => api.get(`/jobs/${id}/metrics`),
  flinkCluster:()                          => api.get('/jobs/flink/cluster'),
}

// ── Agent ─────────────────────────────────────────────────────────────────────
export const agentApi = {
  examples: ()           => api.get('/agent/examples'),
  chat:     (data: any)  => api.post('/agent/chat', data),
  models:   ()           => api.get('/agent/models'),
}

// ── Ontology ──────────────────────────────────────────────────────────────────
export const ontologyApi = {
  listProposals:  (status?: string) => api.get('/ontology/proposals', { params: status ? { status } : {} }),
  proposalStats:  ()                => api.get('/ontology/proposals/stats'),
  getProposal:    (id: number)      => api.get(`/ontology/proposals/${id}`),
  approve:        (id: number)      => api.post(`/ontology/proposals/${id}/approve`),
  reject:         (id: number)      => api.post(`/ontology/proposals/${id}/reject`),
  dags:           ()                => api.get('/ontology/dags'),
  triggerDag:     (dagId: string)   => api.post(`/ontology/dags/${dagId}/trigger`),
  pauseDag:       (dagId: string)   => api.post(`/ontology/dags/${dagId}/pause`),
  schemaStats:    ()                => api.get('/ontology/schema/stats'),
}

// ── Platform ──────────────────────────────────────────────────────────────────
export const platformApi = {
  urls: () => api.get('/platform/urls'),
}
