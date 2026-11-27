/*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import useStore from '@/store'

export type UserPrivilege
  = 'VIEW_SYSTEM'
    | 'VIEW_CATALOG'
    | 'VIEW_TABLE'
    | 'VIEW_OPTIMIZER'
    | 'MANAGE_CATALOG'
    | 'MANAGE_TABLE'
    | 'MANAGE_OPTIMIZER'
    | 'MANAGE_PLATFORM'

export function getRoles(): string[] {
  const store = useStore()
  const roles = store.userInfo.roles || []
  if (roles.length) {
    return roles
  }
  return store.userInfo.role ? [store.userInfo.role] : []
}

export function getPrivileges(): UserPrivilege[] {
  const store = useStore()
  return (store.userInfo.privileges || []) as UserPrivilege[]
}

export function hasPrivilege(privilege: UserPrivilege): boolean {
  return getPrivileges().includes(privilege)
}

export function canViewSystem(): boolean {
  return hasPrivilege('VIEW_SYSTEM')
}

export function canViewCatalog(): boolean {
  return hasPrivilege('VIEW_CATALOG')
}

export function canManageCatalog(): boolean {
  return hasPrivilege('MANAGE_CATALOG')
}

export function canViewTable(): boolean {
  return hasPrivilege('VIEW_TABLE')
}

export function canManageTable(): boolean {
  return hasPrivilege('MANAGE_TABLE')
}

export function canViewOptimizer(): boolean {
  return hasPrivilege('VIEW_OPTIMIZER')
}

export function canManageOptimizer(): boolean {
  return hasPrivilege('MANAGE_OPTIMIZER')
}

export function getDefaultRoute(): string {
  if (canViewTable()) {
    return '/tables'
  }
  if (canViewCatalog()) {
    return '/catalogs'
  }
  if (canViewOptimizer()) {
    return '/optimizing'
  }
  if (canViewSystem()) {
    return '/overview'
  }
  return '/login'
}

export interface RedirectTarget {
  path: string
  query: Record<string, any>
}

function hasControlChar(value: string): boolean {
  for (let i = 0; i < value.length; i += 1) {
    const code = value.charCodeAt(i)
    if (code <= 0x1F || code === 0x7F)
      return true
  }
  return false
}

export function normalizeRoutePath(path: string): string {
  const trimmed = path.replace(/\/+$/, '')
  return (trimmed || '/').toLowerCase()
}

export function isSafeRedirectPath(path: unknown): path is string {
  if (typeof path !== 'string' || !path.startsWith('/')) {
    return false
  }
  if (path.startsWith('//') || path.includes('://') || path.includes('\\') || hasControlChar(path)) {
    return false
  }
  const normalized = normalizeRoutePath(path)
  if (normalized === '/' || normalized === '/login' || normalized === '/404') {
    return false
  }
  return true
}

export function canAccessRoute(path: string): boolean {
  const normalized = normalizeRoutePath(path)
  if (normalized === '/hive-tables/upgrade' || normalized === '/tables/hive-upgrade') {
    return canManageTable()
  }
  if (normalized === '/overview') {
    return canViewSystem()
  }
  if (normalized === '/catalogs') {
    return canViewCatalog()
  }
  if (normalized === '/optimizing') {
    return canViewOptimizer()
  }
  if (normalized === '/tables' || normalized.startsWith('/tables/') || normalized === '/hive-tables' || normalized.startsWith('/hive-tables/')) {
    return canViewTable()
  }
  return true
}

export function capturePreLoginTarget(
  to: { path: string, query?: Record<string, any> },
  from: { path: string, query?: Record<string, any> },
  isAuthenticated: boolean,
): RedirectTarget | null {
  const normalizedTo = normalizeRoutePath(to.path)
  if (!isAuthenticated && normalizedTo !== '/login' && isSafeRedirectPath(to.path)) {
    return { path: to.path, query: { ...(to.query || {}) } }
  }
  if (normalizedTo === '/login' && isSafeRedirectPath(from.path)) {
    return { path: from.path, query: { ...(from.query || {}) } }
  }
  return null
}

export function resolvePostLoginTarget(
  historyPathInfo: { path?: string, query?: Record<string, any> } | null | undefined,
): RedirectTarget {
  const path = historyPathInfo?.path
  if (isSafeRedirectPath(path) && canAccessRoute(path)) {
    return { path, query: { ...(historyPathInfo?.query || {}) } }
  }
  return { path: getDefaultRoute(), query: {} }
}
