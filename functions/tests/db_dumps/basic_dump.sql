--
-- PostgreSQL database dump
--

-- Dumped from database version 12.2
-- Dumped by pg_dump version 12.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: postgis; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;


--
-- Name: EXTENSION postgis; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION postgis IS 'PostGIS geometry, geography, and raster spatial types and functions';


SET default_tablespace = '';

SET default_table_access_method = heap;


--
-- Name: table_1; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.table_1 (
    column_1 character varying(5) NOT NULL,
    column_2 character varying(50) NOT NULL
);


--
-- Data for Name: table_1; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.table_1 (column_1, column_2) FROM stdin;
a	b
y	z
\.

