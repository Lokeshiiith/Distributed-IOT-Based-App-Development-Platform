import logo from "./logo.svg";
import { useEffect, useState, useMemo } from "react";
import "./App.css";
import CustomTable from "./components/CustomTable";

const TABLE_COLUMNS = [
  // { id: "_id", label: "ID", minWidth: 150 },
  { id: "container_name", label: "Container Name", minWidth: 250 },
  { id: "node_name", label: "Node_name", minWidth: 250 },
  { id: "health", label: "Status", minWidth: 150 },
  { id: "current_time", label: "Heartbeat Since", minWidth: 150 },
];

function App() {
  const [data, setData] = useState([]);
  const [currentEpochTime, setCurrentEpochTime] = useState(0);

  useEffect(() => {
    const epochTimer = setInterval(() => {
      setCurrentEpochTime(new Date().getTime() / 1000);
    }, 1000);

    const fetchLogs = () => {
      fetch("http://localhost:5050/logs/health")
        .then((res) => res.json())
        .then((res) => {
          setData(res);
        });
    };
    fetchLogs();
    const intervalId = setInterval(fetchLogs, 5000); // Refresh every 5 seconds

    // Cleanup function to stop the interval when the component unmounts
    return () => {
      clearInterval(intervalId);
      clearInterval(epochTimer);
    };
  }, []);

  const getPayload = () => {
    return [
      ...(data?.available || []).map((record) => {
        return {
          ...record,
          isAlert: false,
          health: "Active",
          current_time: (currentEpochTime - record.current_time).toFixed(0),
        };
      }),
      ...(data?.alert || []).map((record) => {
        return {
          ...record,
          isAlert: true,
          health: "Alert",
          current_time: (currentEpochTime - record.current_time).toFixed(0),
        };
      }),
    ];
  };

  return (
    <div className="monitoring">
      <header>
        <h1 className="heading">Monitoring System</h1>
      </header>
      <div className="details-view">
        <CustomTable columns={TABLE_COLUMNS} data={getPayload()} />
      </div>
    </div>
  );
}

export default App;
