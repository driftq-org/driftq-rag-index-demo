# API Security Basics

Security is layered. In this demo you should think about it like this:

- Authentication: who are you?
- Authorization: what are you allowed to do?
- Audit logs: what happened and when?
- Rate limiting: stop abuse and protect the service
- Tenant isolation: keep customers from seeing each other’s data

This repo does not implement auth on purpose. It is focused on the queue + index pipeline. If you turn this into a real service, start with auth and rate limits first.
