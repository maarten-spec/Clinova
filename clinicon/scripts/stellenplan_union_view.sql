-- Union-View f\u00fcr alle Stellenplan-Tabellen (basierend auf Inventory "Supabase Snippet Public Schema Column Inventory.csv")
-- Ausf\u00fchren in Supabase SQL-Konsole:
--   CREATE OR REPLACE VIEW public.stellenplan_employees_all AS ...

CREATE OR REPLACE VIEW public.stellenplan_employees_all AS
SELECT 'ADMIN' AS site, * FROM public.stellenplan_employees_admin
UNION ALL SELECT 'GFOBAH' AS site, * FROM public.stellenplan_employees_gfobah
UNION ALL SELECT 'GFOBEN' AS site, * FROM public.stellenplan_employees_gfoben
UNION ALL SELECT 'GFOBER' AS site, * FROM public.stellenplan_employees_gfober
UNION ALL SELECT 'GFOBEU' AS site, * FROM public.stellenplan_employees_gfobeu
UNION ALL SELECT 'GFOBRU' AS site, * FROM public.stellenplan_employees_gfobru
UNION ALL SELECT 'GFODIN' AS site, * FROM public.stellenplan_employees_gfodin
UNION ALL SELECT 'GFODUI' AS site, * FROM public.stellenplan_employees_gfodui
UNION ALL SELECT 'GFOENG' AS site, * FROM public.stellenplan_employees_gfoeng
UNION ALL SELECT 'GFOHIL' AS site, * FROM public.stellenplan_employees_gfohil
UNION ALL SELECT 'GFOLAN' AS site, * FROM public.stellenplan_employees_gfolan
UNION ALL SELECT 'GFOLEN' AS site, * FROM public.stellenplan_employees_gfolen
UNION ALL SELECT 'GFOMOE' AS site, * FROM public.stellenplan_employees_gfomoe
UNION ALL SELECT 'GFOOLP' AS site, * FROM public.stellenplan_employees_gfoolp
UNION ALL SELECT 'GFORHE' AS site, * FROM public.stellenplan_employees_gforhe
UNION ALL SELECT 'GFOSIE' AS site, * FROM public.stellenplan_employees_gfosie
UNION ALL SELECT 'GFOTRO' AS site, * FROM public.stellenplan_employees_gfotro
UNION ALL SELECT 'GFOWIS' AS site, * FROM public.stellenplan_employees_gfowis
UNION ALL SELECT 'GFOZPD' AS site, * FROM public.stellenplan_employees_gfozpd;

