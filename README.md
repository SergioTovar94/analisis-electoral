# Análisis Electoral – Rankings de Puestos de Votación

Este proyecto es un sistema de análisis electoral orientado a campañas políticas,
cuyo objetivo es identificar y rankear puestos de votación según distintos criterios
estratégicos (pérdida de votos, votos nulos, fortalezas históricas).

El sistema procesa resultados electorales mesa a mesa provenientes de archivos
CSV y Excel, los normaliza, valida su calidad y genera agregaciones por puesto de
votación para permitir análisis comparativos entre distintas elecciones.

## Objetivos del sistema

- Identificar los puestos de votación donde más votos se perdieron entre dos elecciones
- Identificar los puestos donde históricamente se obtienen buenos resultados
- Identificar los puestos con mayor cantidad de votos nulos
- Realizar cruces entre rankings (ej. alta nulidad y baja pérdida)
- Exportar resultados en formato Excel para su uso operativo en campaña

## Alcance inicial

- Departamento: Cundinamarca
- Nivel de análisis: Puesto de votación
- Fuente de datos: Resultados electorales oficiales del Observatorio de la Registraduría Nacional del Estado Civil (mesa a mesa)
- Tecnologías principales: Python, PostgreSQL, FastAPI

## Público objetivo

- Coordinadores de datos de campaña
- Equipos territoriales
- Analistas políticos

