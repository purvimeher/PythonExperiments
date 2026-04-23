using filter condition in mongo db
{
  $and: [
    { Brand: "OFFICERS CHOICE PRESTIGE WHISKY" },
    { Size_ML: 1000 }
  ]
}

USE BnDeyMainProcessExecutor TO WORK ON CURRENT RUNNING INVENTORY_DB

USE BnDeyMainReportsGenerator TO GENERATE SALES REPORTS


squash git commits and push to remote
git reset --soft $(git rev-list --max-parents=0 HEAD)
git commit -m "Initial clean commit" 
git push --force-with-lease origin main