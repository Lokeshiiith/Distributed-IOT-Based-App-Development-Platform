import { arrayOf, string, shape } from "prop-types";
import Table from "@mui/material/Table";
import TableBody from "@mui/material/TableBody";
import TableCell from "@mui/material/TableCell";
import TableContainer from "@mui/material/TableContainer";
import TableHead from "@mui/material/TableHead";
import TableRow from "@mui/material/TableRow";

const CustomTable = ({ columns, data }) => {
  return (
    <TableContainer sx={{ maxHeight: "80vh" }}>
      <Table stickyHeader aria-label="sticky table">
        <TableHead>
          <TableRow>
            {columns.map((column) => (
              <TableCell
                key={column.id}
                align={column.align}
                style={{
                  minWidth: column.minWidth,
                  background: "#D3D4D9",
                  fontWeight: 600,
                }}
              >
                {column.label}
              </TableCell>
            ))}
          </TableRow>
        </TableHead>
        <TableBody>
          {data.map((row) => {
            return (
              <TableRow key={row._id}>
                {columns.map((column) => {
                  const value = row[column.id];
                  return (
                    <TableCell
                      key={column.id}
                      align={column.align}
                      style={{
                        background: row.isAlert ? "#F98B99" : "#C0EDE1",
                      }}
                    >
                      {value}
                    </TableCell>
                  );
                })}
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </TableContainer>
  );
};

export default CustomTable;

CustomTable.propTypes = {
  columns: arrayOf(shape({})),
  data: arrayOf(shape({})),
  rowBgColor: string,
};

CustomTable.defaultProps = {
  columns: [],
  data: [],
  rowBgColor: "green",
};