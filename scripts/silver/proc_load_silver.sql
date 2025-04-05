/*
================================================================================================
Stored Procedure: Load silver layer (Bronze -> Silver)
===============================================================================================
Script purpose:
       This stored procedure performs the ETL(Extract, Transform, Load) process to populate the
'silver' schema tables from the 'bronze' schema.
  Actions Performed:
-Truncates Silver tables.
-Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure doesnot accept any parameters or return any values.

Usage Example:
Exec silver.load_silver;
===============================================================================================
*/
create procedure silver.load_silver ()
language plpgsql
AS $$
begin
		
--1.insert into silver.crm_cust_info from bronze.crm_cust_info
        truncate table  silver.crm_cust_info;
        insert into silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        )
        select 
        cst_id,
        cst_key,
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname,
        case
        when upper(trim(cst_marital_status)) = 'M' then 'Married'
        when  upper(trim(cst_marital_status)) = 'S' then 'Single'
        else 'n/a'
        end as cst_marital_status,
        when upper(trim(cst_gndr))  = 'M' then 'Male'
        when upper(trim(cst_gndr))  = 'F' then  'Female'
        else 'n/a'
        end as cst_gndr,
        cst_create_date
        from bronze.crm_cust_info;

--2.insert values from bronze.crm_prd_info to silver.crm_prd_info
			truncate table silver.crm_prd_info;
			insert into silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
			select prd_id,
			replace(substring(prd_key from 1 for 5),'-','_') cat_id,
			substring(prd_key from 7 for length(prd_key)) as prd_key,
			prd_nm,
			coalesce(prd_cost,0) as prd_cost,
			case upper(trim(prd_line))
			when 'R' then 'Road'
			when 'S' then 'Other sales'
			when 'M' then 'Mountain'
			when 'T' then 'Touring'
			else 'n/a'
			end as prd_line,
			prd_start_dt,
			case when prd_end_dt > prd_start_dt then prd_end_dt
			else 
			lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 
			end as prd_end_dt
			from bronze.crm_prd_info;
			
--3.insert values from bronze.crm_sales_details
			
			truncate table silver.crm_sales_details;
			insert into silver.crm_sales_details
			SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when length(cast(sls_order_dt as text)) != 8 
			or sls_order_dt = 0 then null
			else cast(cast(sls_order_dt as text) as date)
			end as sls_order_dt,
			case when length(cast(sls_ship_dt as text)) != 8 
			or sls_ship_dt = 0 then null
			else cast(cast(sls_ship_dt as text) as date)
			end as sls_ship_dt,
			case when length(cast(sls_due_dt as text)) != 8
			or sls_due_dt = 0 then null
			else cast(cast(sls_due_dt as text) as date)
			end as sls_due_dt,
			case when sls_sales is null or sls_sales <= 0 
			or sls_sales != sls_quantity * abs(sls_price)
			then sls_quantity * abs(sls_price)
			else sls_sales
			end as sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price <=0
			then sls_sales/nullif(sls_quantity,0)
			else sls_price
			end as sls_price
			from bronze.crm_sales_details;
			
--4.insert values into silver.erp_CUST_AZ12 from table bronze.erp_CUST_AZ12
			truncate table silver.erp_CUST_AZ12;
			
			insert into silver.erp_CUST_AZ12
			select 
			case  when cid like 'NAS%' THEN substring(cid from 4 for length(cid))
			else cid 
			end,
			case when bdate > now() then null
			else bdate
			end as bdate,
			case 
			when upper(trim(gen)) in ('M','MALE') then 'Male'
			when  upper(trim(gen)) in ('F','FEMALE')  then 'Female'
			else 'n/a'
			end as gen
			from bronze.erp_CUST_AZ12;
	
--5.insert values into silver.erp_LOC_A101
			
			truncate table silver.erp_LOC_A101;
			insert into silver.erp_LOC_A101
			select replace(cid,'-','') cid,
			case
			when trim(cntry) in ('US','USA') then 'United States'
			when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) = '' or cntry is null then 'n/a'
			else trim(cntry)
			end as cntry
			from bronze.erp_LOC_A101;
			
--6. insert into silver.erp_PX_CAT_G1V2 from bronze.erp_PX_CAT_G1V2
			
			truncate table silver.erp_PX_CAT_G1V2;
			insert into silver.erp_PX_CAT_G1V2
			select id,
			cat,
			subcat,
			maintenance
			from bronze.erp_PX_CAT_G1V2;
		
end;
$$;
