import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "@/components/ui/select"

export const Filter = ({ setFilter, filter, filterName }) => {
return (
    <Select onValueChange={e => setFilter({
        [filterName.toLowerCase()]: e
        })}>
        <SelectTrigger className="w-[180px]">
          <SelectValue placeholder={filterName} />
        </SelectTrigger>
        <SelectContent>
          {filter.values.map((value) => <SelectItem key={value} value={value}>{value.toUpperCase()}</SelectItem>)}
        </SelectContent>
    </Select>
)
}